package deployment

import (
	"context"
	"strconv"

	sdk "github.com/pipe-cd/piped-plugin-sdk-go"

	"github.com/niladrix719/pipecd-kafka-plugin/config"
)

// executeRegisterSchema runs KAFKA_REGISTER_SCHEMA: it registers the new schema
// versions in the plan, and nothing else.
//
// Compatibility was already checked while the plan was built, so a subject that
// would break its consumers never reaches this stage.
func (p *Plugin) executeRegisterSchema(ctx context.Context, in stageInput) *sdk.ExecuteStageResponse {
	lp := in.lp
	var opts config.RegisterSchemaStageOptions
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

	if registry == nil {
		lp.Success("This deploy target has no schema registry configured, so there is nothing to register.")
		return success()
	}

	built, err := buildPlan(ctx, in.targetSource, in.target, cluster, registry, opts.Compatibility)
	if err != nil {
		lp.Errorf("Failed to build the plan: %v", err)
		return failure()
	}
	if built.HasBlocked() {
		lp.Error(built.Render())
		lp.Errorf("The plan contains %d blocked change(s). Nothing will be registered.", len(built.Blocked))
		return failure()
	}

	changes := built.SchemaChanges()
	if len(changes) == 0 {
		lp.Success("Every declared schema is already the latest version of its subject.")
		return success()
	}

	registered := 0
	for _, change := range changes {
		lp.Infof("Registering: %s", change.Describe())

		result, err := registry.RegisterSchema(ctx, change.Subject, *change.Schema)
		if err != nil {
			lp.Errorf("Failed to register subject %s: %v", change.Subject, err)
			lp.Errorf("%d of %d schema(s) were registered before this failure.", registered, len(changes))
			return failure()
		}
		registered++
		lp.Infof("  registered subject %s as version %d (schema id %d)", result.Subject, result.Version, result.ID)
	}

	if err := in.metadata.PutStageMetadataMulti(ctx, map[string]string{
		metadataKeyApplied: strconv.Itoa(registered),
	}); err != nil {
		lp.Errorf("Failed to record the stage metadata: %v", err)
	}

	lp.Successf("Registered %d schema version(s).", registered)
	return success()
}
