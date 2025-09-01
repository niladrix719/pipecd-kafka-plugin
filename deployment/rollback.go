package deployment

import (
	"context"
	"strconv"

	sdk "github.com/pipe-cd/piped-plugin-sdk-go"

	"github.com/niladrix719/pipecd-kafka-plugin/plan"
)

// executeRollback runs KAFKA_ROLLBACK after a failed deployment.
//
// Rollback is expressed as a deployment of the previously running desired state
// against the cluster as it is now. That handles a partial apply for free: the
// plan simply describes whatever gap is actually left.
//
// What it cannot do is undo an irreversible change. A partition increase cannot
// be reversed, and a deleted topic's data is gone. Those are reported by name
// and skipped, so the log says what was restored and what stayed changed rather
// than implying the world was put back.
func (p *Plugin) executeRollback(ctx context.Context, in stageInput) *sdk.ExecuteStageResponse {
	lp := in.lp
	running := in.runningSource
	if running.ApplicationDirectory == "" {
		lp.Success("There is no previously running revision to roll back to, so there is nothing to restore.")
		return success()
	}

	cluster, registry, err := p.connect(in.target)
	if err != nil {
		lp.Errorf("Failed to connect to the deploy target %q: %v", in.target.Name, err)
		return failure()
	}
	defer cluster.Close()

	// Build the plan from the previous desired state: what would it take to get
	// the cluster back to how the last successful deployment left it?
	built, err := buildPlan(ctx, running, in.target, cluster, registry, "")
	if err != nil {
		lp.Errorf("Failed to build the rollback plan: %v", err)
		return failure()
	}

	restorable := make([]plan.Change, 0, len(built.Changes))
	var skipped []plan.Change
	for _, change := range built.TopicChanges() {
		if change.Kind.Reversible() {
			restorable = append(restorable, change)
			continue
		}
		skipped = append(skipped, change)
	}

	// Blocked changes are reported but do not fail the rollback: refusing to
	// restore part of the state is better than leaving all of it changed.
	for _, blocked := range built.Blocked {
		lp.Errorf("Cannot restore: %s", blocked.Reason)
	}
	for _, change := range skipped {
		lp.Errorf("Cannot be undone, leaving as it is: %s", change.Describe())
	}

	if len(restorable) == 0 {
		if len(skipped) == 0 && len(built.Blocked) == 0 {
			lp.Success("The cluster already matches the previously running revision.")
			return success()
		}
		lp.Error("Nothing in this deployment could be rolled back automatically.")
		return failure()
	}

	applied, failed := applyChanges(ctx, cluster, restorable, lp, true)

	if err := in.metadata.PutStageMetadataMulti(ctx, map[string]string{
		metadataKeyApplied:      strconv.Itoa(applied),
		metadataKeyIrreversible: strconv.Itoa(len(skipped)),
	}); err != nil {
		lp.Errorf("Failed to record the stage metadata: %v", err)
	}

	if failed > 0 {
		lp.Errorf("Restored %d change(s), but %d could not be restored.", applied, failed)
		return failure()
	}

	if len(skipped) > 0 || len(built.Blocked) > 0 {
		lp.Errorf("Restored %d change(s). The cluster does not fully match the previous revision, because some changes cannot be undone.", applied)
		return failure()
	}

	lp.Successf("Restored %d change(s). The cluster matches the previously running revision.", applied)
	return success()
}
