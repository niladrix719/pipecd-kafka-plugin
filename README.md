# pipecd-kafka-plugin

A [PipeCD](https://pipecd.dev) deployment plugin that manages **Kafka topics and schemas as a deploy
target**. Topic definitions and Avro/Protobuf schemas live in git; piped plans the difference against
the real cluster, checks schema compatibility with the registry, and applies it.

Built on [`piped-plugin-sdk-go`](https://github.com/pipe-cd/piped-plugin-sdk-go) for the
plugin-arched piped (`pipedv1`).

**New here?** [`docs/GETTING_STARTED.md`](docs/GETTING_STARTED.md) walks through running this against
a real PipeCD control plane and a real Kafka cluster on your own machine, end to end.

## How it works

PipeCD is pull-based: an agent called **piped** runs in your infrastructure and watches a git repo. It
never gets pushed to. This plugin is what piped calls when an application's pipeline reaches a
`KAFKA_*` stage.

```
git repo                     piped                          this plugin           Kafka
───────────                  ─────                          ───────────           ─────
topics/orders.yaml    ──►    detects a new commit     ──►   KAFKA_PLAN      ──►   read actual state
schemas/orders.avsc          checks out the old and         diff desired          check schema
                              new commit                     vs. actual            compatibility
                                                              │
                                                    blocked?  │  clean
                                                   ┌──────────┴──────────┐
                                              stop, nothing          KAFKA_REGISTER_SCHEMA
                                              is touched                    │
                                                                       KAFKA_APPLY   ──►   create/alter/
                                                                                            delete topics
```

Three things fall out of that shape:

- **The diff is between commits, not files you edited.** piped checks out the application directory at
  the *previously deployed* commit and at the *target* commit, and the plugin diffs those two — so an
  accumulation of several merges since the last deploy shows up as one plan, not one PR at a time.
- **`KAFKA_PLAN` runs before anything is touched.** It reads the real cluster and the real registry,
  classifies every change by whether it's reversible, and fails the whole deployment if it contains an
  irreversible operation the deploy target hasn't explicitly permitted (`allowPartitionIncrease`,
  `allowTopicDeletion`). See [`plan/change.go`](plan/change.go) for exactly what's reversible and why.
- **Schema registration is its own stage**, separate from applying topic changes, specifically so an
  operator can order it against a service rollout in the pipeline — register a backward-compatible
  schema before consumers update, for instance.

## Why this is not just "apply the YAML"

Kafka is an unusually unforgiving deploy target, because **some changes cannot be undone**:

| Operation | Reversible? | What the plugin does |
| --- | --- | --- |
| Create a topic | Yes — delete it | Applied |
| Change a topic config | Yes — restore the old values | Applied, previous values recorded for rollback |
| Register a schema version | Weakly — the version can be soft-deleted | Applied after a compatibility check |
| **Increase partitions** | **No** — Kafka cannot lower a partition count, and it changes which partition a key hashes to | Refused unless the deploy target opts in |
| **Delete a topic** | **No** — the data is gone | Refused unless the deploy target opts in |

So the plan stage is load-bearing rather than decorative. It classifies every change by
reversibility, refuses anything the cluster's deploy target has not explicitly permitted, and fails
the deployment **before a single change is applied**. And rollback tells the truth: it restores what
it can and names, in the log, exactly what it could not undo.

Two other things fall out of taking Kafka seriously:

- **A schema that would break consumers never gets registered.** Compatibility is checked against the
  registry while the plan is being built, so an incompatible schema blocks the deployment instead of
  being discovered by a consumer at 3am.
- **Registration is its own stage.** `KAFKA_REGISTER_SCHEMA` is separate from `KAFKA_APPLY` so it can
  be ordered against the rollout of your services: a backward-compatible schema should reach the
  registry before consumers are updated, a forward-compatible one before producers are. Folding it
  into apply would make that ordering impossible to express.

## Stages

| Stage | What it does |
| --- | --- |
| `KAFKA_PLAN` | Diffs desired against actual, checks schema compatibility, reports the plan. Fails if anything is blocked. Changes nothing. |
| `KAFKA_REGISTER_SCHEMA` | Registers the new schema versions in the plan. |
| `KAFKA_APPLY` | Creates, updates and deletes topics to match the desired state. |
| `KAFKA_ROLLBACK` | Added automatically. Restores the previously running state as far as Kafka allows. |

## Configuration

### Deploy target — one Kafka cluster

The safety rails live on the deploy target, not the application. The same application config is
deployed to staging and to production; a rail defined per application would travel with the change
into production.

```yaml
plugins:
  - name: kafka
    port: 7020
    url: https://github.com/niladrix719/pipecd-kafka-plugin/releases/download/v0.1.0/plugin_kafka_v0.1.0_linux_amd64
    deployTargets:
      - name: prod
        config:
          bootstrapServers: ["b1.prod:9092", "b2.prod:9092"]
          tls:
            enabled: true
          sasl:
            mechanism: SCRAM-SHA-512
            username: piped
            passwordFile: /etc/piped/kafka-password
          schemaRegistry:
            url: https://registry.prod:8081
          allowTopicDeletion: false
          allowPartitionIncrease: false
          protectedTopics: ["__*", "*.dlq"]
```

| Field | Default | Description |
| --- | --- | --- |
| `bootstrapServers` | — | Broker addresses. Required. |
| `clientID` | — | Identifies this piped in broker logs and quotas. |
| `tls` | disabled | `enabled`, `caFile`, `certFile`, `keyFile`, `insecureSkipVerify`. |
| `sasl` | disabled | `mechanism` (`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`), `username`, and `password` or `passwordFile`. |
| `schemaRegistry` | none | `url`, optional `username` and `password`/`passwordFile`, `caFile`. Declaring a schema without this is an error. |
| `allowTopicDeletion` | `false` | Permit deleting topics that are no longer declared. |
| `allowPartitionIncrease` | `false` | Permit raising partition counts. |
| `protectedTopics` | — | Globs this piped must never modify, whatever the application declares. |
| `driftDetectionEnabled` | `true` | Compare live state against desired state outside a deployment. |

### Application — a slice of a shared cluster

```yaml
spec:
  plugins:
    kafka:
      topicsDir: topics
      schemasDir: schemas
      ownership:
        - orders.*
        - payments.*
      defaults:
        replicationFactor: 3
        config:
          min.insync.replicas: "2"
```

`ownership` is what makes deletion expressible without being catastrophic. A Kafka cluster is shared
by many applications; without a scope, a plan would look at every topic on the cluster and propose
deleting the ones it has no file for. Topics outside the scope are invisible to this application, and
declaring a topic outside it is an error rather than a silent orphan.

### A topic definition

```yaml
# topics/orders.yaml
name: orders
partitions: 12
replicationFactor: 3
config:
  retention.ms: "604800000"
  cleanup.policy: delete
schema:
  subject: orders-value
  file: orders.avsc      # relative to schemasDir
  compatibility: BACKWARD
```

One file may hold several topics as a YAML stream. A config key that was set and is no longer
declared is reset to the broker default, not left behind.

### Stage options

```yaml
- name: KAFKA_PLAN
  with:
    exitOnNoChanges: true    # end the deployment successfully when nothing would change
- name: KAFKA_APPLY
  with:
    continueOnError: false   # stop at the first failure (default), or keep applying independent changes
```

## What a plan looks like

```
Plan: 1 to create topic, 1 to update config, 1 to increase partitions.

  + create topic orders (12 partitions, replication factor 3)
  ~ update config of topic payments (retention.ms: 604800000 -> 2592000000)
  ! increase partitions of topic events from 6 to 12

These changes cannot be undone by a rollback:
  ! increase partitions of topic events from 6 to 12
```

and when something is refused:

```
Blocked (1). Nothing will be applied until these are resolved:
  x the new schema for subject orders-value is not compatible with version 4: reader field 'total' is missing a default value
```

## Local development

```sh
make up       # Redpanda + schema registry + console on localhost
make test     # go test -failfast -race ./...
make build    # build the plugin binary
make down
```

Redpanda is Kafka API compatible and starts in seconds, so the code above can be exercised against a
real broker without a managed cluster. `examples/simple` is a ready-made application directory.

The test suite needs none of it: the cluster and registry are behind interfaces, with in-memory
implementations in `provider` that enforce the same rules the real ones do (partition counts only go
up, a deleted topic is really gone).

`make up` above only starts Kafka — for the full loop with a real PipeCD control plane and piped
actually deploying this plugin's plan, see [`docs/GETTING_STARTED.md`](docs/GETTING_STARTED.md), which
uses `./hack/local-env.sh` to bring up everything else.

## Status

Early. The plan, apply, schema registration and rollback paths are implemented and tested; drift
detection and the live-state view are not wired up yet.

## License

[Apache 2.0](LICENSE).
