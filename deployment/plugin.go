// Package deployment implements the PipeCD deployment plugin: the stages that
// plan, register schemas for, apply and roll back an application's Kafka state.
package deployment

import (
	"context"
	"encoding/json"
	"fmt"

	sdk "github.com/pipe-cd/piped-plugin-sdk-go"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
	"github.com/niladrix719/pipecd-kafka-plugin/plan"
	"github.com/niladrix719/pipecd-kafka-plugin/provider"
)

// The stages this plugin defines.
//
// Registration is a separate stage from apply so that it can be ordered against
// the rollout of the services that produce and consume the topic. A
// backward-compatible schema should be registered before consumers are updated;
// a forward-compatible one before producers are. Folding registration into
// apply would make that ordering impossible to express.
const (
	StagePlan           = "KAFKA_PLAN"
	StageRegisterSchema = "KAFKA_REGISTER_SCHEMA"
	StageApply          = "KAFKA_APPLY"
	StageRollback       = "KAFKA_ROLLBACK"
)

// Stage metadata keys, shown on the deployment page.
const (
	metadataKeyChanges      = "kafka.changes"
	metadataKeyBlocked      = "kafka.blocked"
	metadataKeyIrreversible = "kafka.irreversible"
	metadataKeyApplied      = "kafka.applied"
)

// metadataStore is the part of *sdk.Client the stages write to. It is an
// interface so a stage can be tested without a live piped.
type metadataStore interface {
	PutStageMetadataMulti(ctx context.Context, metadata map[string]string) error
}

// stageInput is what a stage actually needs from the SDK request, narrowed so
// the stages can be driven directly by tests.
type stageInput struct {
	lp            sdk.StageLogPersister
	metadata      metadataStore
	stageConfig   []byte
	target        *sdk.DeployTarget[config.DeployTargetConfig]
	targetSource  sdk.DeploymentSource[config.ApplicationConfigSpec]
	runningSource sdk.DeploymentSource[config.ApplicationConfigSpec]
}

// Plugin implements the PipeCD deployment plugin.
type Plugin struct {
	// newCluster and newRegistry are overridden in tests so the stages can run
	// against an in-memory cluster instead of a broker.
	newCluster  func(config.DeployTargetConfig) (provider.Cluster, error)
	newRegistry func(config.SchemaRegistryConfig) (provider.Registry, error)
}

var _ sdk.DeploymentPlugin[config.Config, config.DeployTargetConfig, config.ApplicationConfigSpec] = (*Plugin)(nil)

// FetchDefinedStages returns the stages this plugin can execute.
func (p *Plugin) FetchDefinedStages() []string {
	return []string{StagePlan, StageRegisterSchema, StageApply, StageRollback}
}

// BuildPipelineSyncStages builds the stages executed for a pipeline sync,
// appending the rollback stage that runs if any of them fails.
func (p *Plugin) BuildPipelineSyncStages(_ context.Context, _ *config.Config, input *sdk.BuildPipelineSyncStagesInput) (*sdk.BuildPipelineSyncStagesResponse, error) {
	stages := make([]sdk.PipelineStage, 0, len(input.Request.Stages)+1)
	for _, s := range input.Request.Stages {
		stages = append(stages, sdk.PipelineStage{
			Index:              s.Index,
			Name:               s.Name,
			Rollback:           false,
			Metadata:           map[string]string{},
			AvailableOperation: sdk.ManualOperationNone,
		})
	}

	if len(input.Request.Stages) > 0 {
		stages = append(stages, sdk.PipelineStage{
			Index:              input.Request.Stages[0].Index,
			Name:               StageRollback,
			Rollback:           true,
			Metadata:           map[string]string{},
			AvailableOperation: sdk.ManualOperationNone,
		})
	}

	return &sdk.BuildPipelineSyncStagesResponse{Stages: stages}, nil
}

// BuildQuickSyncStages builds the stages for a quick sync, which applies the
// desired state in one step without a separate plan stage.
func (p *Plugin) BuildQuickSyncStages(_ context.Context, _ *config.Config, input *sdk.BuildQuickSyncStagesInput) (*sdk.BuildQuickSyncStagesResponse, error) {
	stages := []sdk.QuickSyncStage{
		{
			Name:               StageRegisterSchema,
			Description:        "Register any new schema versions declared by the topics.",
			Rollback:           false,
			Metadata:           map[string]string{},
			AvailableOperation: sdk.ManualOperationNone,
		},
		{
			Name:               StageApply,
			Description:        "Create, update and delete topics to match the desired state.",
			Rollback:           false,
			Metadata:           map[string]string{},
			AvailableOperation: sdk.ManualOperationNone,
		},
	}
	if input.Request.Rollback {
		stages = append(stages, sdk.QuickSyncStage{
			Name:               StageRollback,
			Description:        "Restore the previously deployed state, as far as it can be restored.",
			Rollback:           true,
			Metadata:           map[string]string{},
			AvailableOperation: sdk.ManualOperationNone,
		})
	}
	return &sdk.BuildQuickSyncStagesResponse{Stages: stages}, nil
}

// DetermineVersions reports the artifact versions of a deployment.
//
// Topics have no version of their own, and the schema versions that would serve
// as one are assigned by the registry at registration time, which is after this
// is called. There is nothing honest to report here.
func (p *Plugin) DetermineVersions(_ context.Context, _ *config.Config, _ *sdk.DetermineVersionsInput[config.ApplicationConfigSpec]) (*sdk.DetermineVersionsResponse, error) {
	return &sdk.DetermineVersionsResponse{}, nil
}

// DetermineStrategy leaves the sync strategy to PipeCD's common logic.
func (p *Plugin) DetermineStrategy(_ context.Context, _ *config.Config, _ *sdk.DetermineStrategyInput[config.ApplicationConfigSpec]) (*sdk.DetermineStrategyResponse, error) {
	return nil, nil
}

// ExecuteStage executes one stage of a deployment.
func (p *Plugin) ExecuteStage(ctx context.Context, _ *config.Config, targets []*sdk.DeployTarget[config.DeployTargetConfig], input *sdk.ExecuteStageInput[config.ApplicationConfigSpec]) (*sdk.ExecuteStageResponse, error) {
	lp := input.Client.LogPersister()

	target, err := singleTarget(targets)
	if err != nil {
		lp.Errorf("%v", err)
		return failure(), nil
	}
	if err := target.Config.Validate(); err != nil {
		lp.Errorf("Invalid deploy target %q: %v", target.Name, err)
		return failure(), nil
	}

	in := stageInput{
		lp:            lp,
		metadata:      input.Client,
		stageConfig:   input.Request.StageConfig,
		target:        target,
		targetSource:  input.Request.TargetDeploymentSource,
		runningSource: input.Request.RunningDeploymentSource,
	}

	switch input.Request.StageName {
	case StagePlan:
		return p.executePlan(ctx, in), nil
	case StageRegisterSchema:
		return p.executeRegisterSchema(ctx, in), nil
	case StageApply:
		return p.executeApply(ctx, in), nil
	case StageRollback:
		return p.executeRollback(ctx, in), nil
	default:
		return nil, fmt.Errorf("unsupported stage: %s", input.Request.StageName)
	}
}

// singleTarget requires exactly one deploy target: an application's topics live
// on one cluster, and applying the same plan to several would need a per-cluster
// plan and rollback.
func singleTarget(targets []*sdk.DeployTarget[config.DeployTargetConfig]) (*sdk.DeployTarget[config.DeployTargetConfig], error) {
	switch len(targets) {
	case 1:
		return targets[0], nil
	case 0:
		return nil, fmt.Errorf("no deploy target was given; this application must name exactly one Kafka cluster")
	default:
		return nil, fmt.Errorf("%d deploy targets were given; this plugin applies to exactly one Kafka cluster at a time", len(targets))
	}
}

// connect opens the cluster and, when configured, the registry.
func (p *Plugin) connect(target *sdk.DeployTarget[config.DeployTargetConfig]) (provider.Cluster, provider.Registry, error) {
	newCluster := p.newCluster
	if newCluster == nil {
		newCluster = provider.NewCluster
	}
	cluster, err := newCluster(target.Config)
	if err != nil {
		return nil, nil, err
	}

	if target.Config.SchemaRegistry == nil {
		return cluster, nil, nil
	}

	newRegistry := p.newRegistry
	if newRegistry == nil {
		newRegistry = provider.NewRegistry
	}
	registry, err := newRegistry(*target.Config.SchemaRegistry)
	if err != nil {
		cluster.Close()
		return nil, nil, err
	}
	return cluster, registry, nil
}

// buildPlan loads the desired state from a deployment source and compares it
// against the cluster.
func buildPlan(ctx context.Context, source sdk.DeploymentSource[config.ApplicationConfigSpec], target *sdk.DeployTarget[config.DeployTargetConfig], cluster provider.Cluster, registry provider.Registry, compatibility string) (*plan.Plan, error) {
	spec, err := source.AppConfig()
	if err != nil {
		return nil, fmt.Errorf("reading the application config: %w", err)
	}

	desired, err := provider.LoadTopics(source.ApplicationDirectory, *spec.Spec)
	if err != nil {
		return nil, err
	}

	actual, err := cluster.ListTopics(ctx)
	if err != nil {
		return nil, err
	}

	return plan.Build(ctx, plan.Input{
		Desired:               desired,
		Actual:                actual,
		Spec:                  *spec.Spec,
		Target:                target.Config,
		Registry:              registry,
		CompatibilityOverride: compatibility,
	})
}

// decodeStageOptions unmarshals a stage's `with` block. An omitted block arrives
// empty rather than as "{}".
func decodeStageOptions(raw []byte, into any) error {
	if len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("decoding the stage config: %w", err)
	}
	return nil
}

func success() *sdk.ExecuteStageResponse {
	return &sdk.ExecuteStageResponse{Status: sdk.StageStatusSuccess}
}

func failure() *sdk.ExecuteStageResponse {
	return &sdk.ExecuteStageResponse{Status: sdk.StageStatusFailure}
}

func exited() *sdk.ExecuteStageResponse {
	return &sdk.ExecuteStageResponse{Status: sdk.StageStatusExited}
}
