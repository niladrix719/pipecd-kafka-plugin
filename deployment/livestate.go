package deployment

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	sdk "github.com/pipe-cd/piped-plugin-sdk-go"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
	"github.com/niladrix719/pipecd-kafka-plugin/plan"
	"github.com/niladrix719/pipecd-kafka-plugin/provider"
)

// The resource kinds shown in the live-state view.
const (
	resourceTypeTopic   = "Topic"
	resourceTypeSubject = "SchemaSubject"
)

var _ sdk.LivestatePlugin[config.Config, config.DeployTargetConfig, config.ApplicationConfigSpec] = (*Plugin)(nil)

// GetLivestate reports what is on the cluster right now, and whether it still
// matches what the repository declares.
//
// piped calls this on a timer, outside any deployment, so every path here only
// reads. Drift is decided by building the same plan KAFKA_PLAN would build: a
// plan that would change nothing means the application is synced. That keeps
// the two answers from ever disagreeing, which is the whole point of showing a
// drift status next to a deploy button.
func (p *Plugin) GetLivestate(ctx context.Context, _ *config.Config, targets []*sdk.DeployTarget[config.DeployTargetConfig], input *sdk.GetLivestateInput[config.ApplicationConfigSpec]) (*sdk.GetLivestateResponse, error) {
	target, err := singleTarget(targets)
	if err != nil {
		return nil, err
	}
	if err := target.Config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid deploy target %q: %w", target.Name, err)
	}

	cluster, registry, err := p.connect(target)
	if err != nil {
		return nil, fmt.Errorf("connecting to the deploy target %q: %w", target.Name, err)
	}
	defer cluster.Close()

	// Without the cluster's own view there is nothing to report at all, so this
	// is the one failure that has to propagate.
	actual, err := cluster.ListTopics(ctx)
	if err != nil {
		return nil, err
	}

	source := input.Request.DeploymentSource
	spec, err := source.AppConfig()
	if err != nil {
		return &sdk.GetLivestateResponse{
			LiveState: liveState(ctx, target.Name, nil, actual, config.ApplicationConfigSpec{}, registry),
			SyncState: invalidConfig(fmt.Errorf("reading the application config: %w", err)),
		}, nil
	}

	// The desired state is loaded before the plan so a malformed topic file is
	// reported as a config problem rather than as an unknown drift status.
	desired, err := provider.LoadTopics(source.ApplicationDirectory, *spec.Spec)
	if err != nil {
		return &sdk.GetLivestateResponse{
			LiveState: liveState(ctx, target.Name, nil, actual, *spec.Spec, registry),
			SyncState: invalidConfig(err),
		}, nil
	}

	live := liveState(ctx, target.Name, desired, actual, *spec.Spec, registry)

	if !target.Config.DriftDetectionEnabledOrDefault() {
		return &sdk.GetLivestateResponse{
			LiveState: live,
			SyncState: sdk.ApplicationSyncState{
				Status:      sdk.ApplicationSyncStateUnknown,
				ShortReason: "Drift detection is disabled on this deploy target.",
				Reason:      "driftDetectionEnabled is false, so the desired state was not compared against the cluster. The topics above are still the live state.",
			},
		}, nil
	}

	built, err := plan.Build(ctx, plan.Input{
		Desired:  desired,
		Actual:   actual,
		Spec:     *spec.Spec,
		Target:   target.Config,
		Registry: registry,
	})
	if err != nil {
		// Usually a registry that is briefly unreachable. Reporting that as
		// out-of-sync would be a false alarm an operator would act on.
		return &sdk.GetLivestateResponse{
			LiveState: live,
			SyncState: sdk.ApplicationSyncState{
				Status:      sdk.ApplicationSyncStateUnknown,
				ShortReason: "The desired state could not be compared against the cluster.",
				Reason:      err.Error(),
			},
		}, nil
	}

	return &sdk.GetLivestateResponse{LiveState: live, SyncState: syncState(built)}, nil
}

// syncState turns a plan into the status shown on the application page.
func syncState(built *plan.Plan) sdk.ApplicationSyncState {
	if built.Empty() {
		return sdk.ApplicationSyncState{
			Status:      sdk.ApplicationSyncStateSynced,
			ShortReason: "The cluster matches the desired state.",
		}
	}

	// A blocked change is still drift: the cluster differs from the repository
	// and a deployment would not close the gap on its own.
	var short string
	switch {
	case len(built.Blocked) == 0:
		short = fmt.Sprintf("%d change(s) would be needed to match the desired state.", len(built.Changes))
	case len(built.Changes) == 0:
		short = fmt.Sprintf("%d change(s) are blocked by this deploy target.", len(built.Blocked))
	default:
		short = fmt.Sprintf("%d change(s) would be needed to match the desired state, and %d more are blocked by this deploy target.", len(built.Changes), len(built.Blocked))
	}

	return sdk.ApplicationSyncState{
		Status:      sdk.ApplicationSyncStateOutOfSync,
		ShortReason: short,
		Reason:      built.Render(),
	}
}

func invalidConfig(err error) sdk.ApplicationSyncState {
	return sdk.ApplicationSyncState{
		Status:      sdk.ApplicationSyncStateInvalidConfig,
		ShortReason: "The desired state could not be read.",
		Reason:      err.Error(),
	}
}

// liveState lists what this application owns on the cluster.
//
// Both sides are listed: a topic that exists but is no longer declared is still
// this application's to clean up, and a topic that is declared but missing is
// the more urgent of the two to see.
func liveState(ctx context.Context, targetName string, desired []provider.Topic, actual map[string]provider.TopicState, spec config.ApplicationConfigSpec, registry provider.Registry) sdk.ApplicationLiveState {
	declared := make(map[string]provider.Topic, len(desired))
	names := make([]string, 0, len(desired)+len(actual))
	for _, topic := range desired {
		declared[topic.Name] = topic
		names = append(names, topic.Name)
	}
	for name := range actual {
		// Everything else on a shared cluster belongs to another application.
		if _, isDeclared := declared[name]; isDeclared || !provider.Matches(name, spec.Ownership) {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	resources := make([]sdk.ResourceState, 0, len(names))
	for _, name := range names {
		topicID := "topic/" + name
		state, exists := actual[name]
		topic, isDeclared := declared[name]

		resource := sdk.ResourceState{
			ID:           topicID,
			Name:         name,
			ResourceType: resourceTypeTopic,
			DeployTarget: targetName,
			HealthStatus: sdk.ResourceHealthStateHealthy,
		}

		if exists {
			resource.ResourceMetadata = topicMetadata(state)
			if !isDeclared {
				resource.HealthDescription = "This topic is no longer declared in the repository."
			}
		} else {
			// Only a declared name can be missing: an undeclared one was read
			// off the cluster in the first place.
			resource.HealthStatus = sdk.ResourceHealthStateUnhealthy
			resource.HealthDescription = "Declared in the repository but not present on the cluster."
			resource.ResourceMetadata = map[string]string{
				"partitions":        strconv.Itoa(topic.Partitions),
				"replicationFactor": strconv.Itoa(topic.ReplicationFactor),
			}
		}
		resources = append(resources, resource)

		if isDeclared && topic.Schema != nil {
			resources = append(resources, subjectResource(ctx, targetName, topicID, topic, registry))
		}
	}

	return sdk.ApplicationLiveState{Resources: resources}
}

// subjectResource reports a topic's registry subject as a child of the topic.
func subjectResource(ctx context.Context, targetName, topicID string, topic provider.Topic, registry provider.Registry) sdk.ResourceState {
	resource := sdk.ResourceState{
		ID:           "subject/" + topic.Schema.Subject,
		ParentIDs:    []string{topicID},
		Name:         topic.Schema.Subject,
		ResourceType: resourceTypeSubject,
		DeployTarget: targetName,
		ResourceMetadata: map[string]string{
			"topic": topic.Name,
			"type":  topic.Schema.SchemaType(),
		},
	}

	if registry == nil {
		resource.HealthStatus = sdk.ResourceHealthStateUnhealthy
		resource.HealthDescription = "This topic declares a schema, but the deploy target has no schemaRegistry configured."
		return resource
	}

	latest, exists, err := registry.LatestSchema(ctx, topic.Schema.Subject)
	switch {
	case err != nil:
		// The topic's own state is still known; only the subject is in doubt.
		resource.HealthStatus = sdk.ResourceHealthStateUnknown
		resource.HealthDescription = fmt.Sprintf("The registry could not be read: %v", err)
	case !exists:
		resource.HealthStatus = sdk.ResourceHealthStateUnhealthy
		resource.HealthDescription = "Declared in the repository but not registered yet."
	default:
		resource.HealthStatus = sdk.ResourceHealthStateHealthy
		resource.ResourceMetadata["version"] = strconv.Itoa(latest.Version)
		resource.ResourceMetadata["id"] = strconv.Itoa(latest.ID)
	}
	return resource
}

// topicMetadata renders a topic's live state as the key/value pairs shown when
// a resource is opened. Dynamic configs are prefixed so they cannot collide
// with the fields above them.
func topicMetadata(state provider.TopicState) map[string]string {
	metadata := map[string]string{
		"partitions":        strconv.Itoa(state.Partitions),
		"replicationFactor": strconv.Itoa(state.ReplicationFactor),
	}
	for key, value := range state.Config {
		metadata["config."+key] = value
	}
	return metadata
}
