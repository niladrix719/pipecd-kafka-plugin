# Changelog

## v1.0.12

- Live-state view: the topics an application owns, with their partition count, replication factor
  and dynamic configs, and each topic's registry subject as a child resource.
- Drift detection between deployments, decided by the same plan `KAFKA_PLAN` builds. A briefly
  unreachable registry reports unknown rather than a false drift alarm. Honors
  `driftDetectionEnabled` on the deploy target.

## v0.1.0

First tagged release.

- Kafka topics and Schema Registry subjects as a PipeCD deploy target, with
  `KAFKA_PLAN`, `KAFKA_REGISTER_SCHEMA`, `KAFKA_APPLY` and `KAFKA_ROLLBACK` stages.
- Deploy-target safety rails: `allowTopicDeletion`, `protectedTopics`,
  `maxPartitionDecrease`, `driftDetectionEnabled`.
- Per-OS, per-arch binaries published by the `release` workflow.
