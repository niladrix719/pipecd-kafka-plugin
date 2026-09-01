package deployment

import (
	"context"
	"strconv"

	sdk "github.com/pipe-cd/piped-plugin-sdk-go"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
)

// executePlan runs KAFKA_PLAN: it compares the desired state against the
// cluster and reports what would change, without touching anything.
func (p *Plugin) executePlan(ctx context.Context, in stageInput) *sdk.ExecuteStageResponse {
	lp := in.lp
	var opts config.PlanStageOptions
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

	lp.Info(built.Render())

	if err := in.metadata.PutStageMetadataMulti(ctx, map[string]string{
		metadataKeyChanges:      strconv.Itoa(len(built.Changes)),
		metadataKeyBlocked:      strconv.Itoa(len(built.Blocked)),
		metadataKeyIrreversible: strconv.Itoa(len(built.Irreversible())),
	}); err != nil {
		lp.Errorf("Failed to record the plan metadata: %v", err)
	}

	if built.HasBlocked() {
		lp.Errorf("The plan contains %d change(s) this deploy target does not permit. Nothing has been applied.", len(built.Blocked))
		return failure()
	}

	if built.Empty() {
		if opts.ExitOnNoChanges {
			lp.Success("No changes. Ending the deployment here because exitOnNoChanges is set.")
			return exited()
		}
		lp.Success("No changes.")
		return success()
	}

	if irreversible := built.Irreversible(); len(irreversible) > 0 {
		lp.Infof("%d change(s) in this plan cannot be undone by a rollback. They are permitted by this deploy target, so the deployment will continue.", len(irreversible))
	}

	lp.Successf("The plan is applicable: %d change(s) to make.", len(built.Changes))
	return success()
}
