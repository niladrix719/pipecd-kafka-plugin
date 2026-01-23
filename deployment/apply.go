package deployment

import (
	"context"
	"fmt"
	"strconv"

	sdk "github.com/pipe-cd/piped-plugin-sdk-go"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
	"github.com/niladrix719/pipecd-kafka-plugin/plan"
	"github.com/niladrix719/pipecd-kafka-plugin/provider"
)

// executeApply runs KAFKA_APPLY: it makes the topic changes in the plan.
func (p *Plugin) executeApply(ctx context.Context, in stageInput) *sdk.ExecuteStageResponse {
	lp := in.lp
	var opts config.ApplyStageOptions
	if err := decodeStageOptions(in.stageConfig, &opts); err != nil {
		lp.Errorf("%v", err)
		return failure()
	}

	cluster, registry, err := p.connect(in.target)
	if err != nil {
		lp.Errorf("Failed to connect to the deploy target %q: %v", in.target.Name, err)
		return failure()
	}
	defer cluster.Close()

	built, err := buildPlan(ctx, in.targetSource, in.target, cluster, registry, "")
	if err != nil {
		lp.Errorf("Failed to build the plan: %v", err)
		return failure()
	}
	if built.HasBlocked() {
		lp.Error(built.Render())
		lp.Errorf("The plan contains %d blocked change(s). Nothing will be applied.", len(built.Blocked))
		return failure()
	}

	changes := built.TopicChanges()
	if len(changes) == 0 {
		lp.Success("The cluster already matches the desired state.")
		return success()
	}

	applied, failed := applyChanges(ctx, cluster, changes, lp, opts.ContinueOnError)

	if err := in.metadata.PutStageMetadataMulti(ctx, map[string]string{
		metadataKeyApplied: strconv.Itoa(applied),
	}); err != nil {
		lp.Errorf("Failed to record the stage metadata: %v", err)
	}

	if failed > 0 {
		// An apply is not atomic. Say plainly how far it got, because that is
		// what determines what a rollback can and cannot restore.
		lp.Errorf("%d of %d change(s) were applied before failing; %d failed.", applied, len(changes), failed)
		return failure()
	}

	lp.Successf("Applied %d change(s).", applied)
	return success()
}

func applyChanges(ctx context.Context, cluster provider.Cluster, changes []plan.Change, lp sdk.StageLogPersister, continueOnError bool) (applied, failed int) {
	for _, change := range changes {
		lp.Infof("Applying: %s", change.Describe())

		if err := applyChange(ctx, cluster, change); err != nil {
			failed++
			lp.Errorf("  failed: %v", err)
			if !continueOnError {
				return applied, failed
			}
			continue
		}
		applied++
	}
	return applied, failed
}

func applyChange(ctx context.Context, cluster provider.Cluster, change plan.Change) error {
	switch change.Kind {
	case plan.CreateTopic:
		return cluster.CreateTopic(ctx, *change.Desired)
	case plan.UpdateTopicConfig:
		return cluster.AlterTopicConfig(ctx, change.Topic, change.ConfigChange)
	case plan.IncreasePartitions:
		return cluster.IncreasePartitions(ctx, change.Topic, change.ToPartitions)
	case plan.DeleteTopic:
		return cluster.DeleteTopic(ctx, change.Topic)
	case plan.RegisterSchema:
		// Schemas are registered by their own stage.
		return fmt.Errorf("a schema change reached the apply stage, which only applies topic changes")
	default:
		return fmt.Errorf("unknown change kind %v", change.Kind)
	}
}
