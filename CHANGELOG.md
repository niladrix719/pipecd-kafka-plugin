# Changelog

## v0.1.0

First tagged release.

- Kafka topics and Schema Registry subjects as a PipeCD deploy target, with
  `KAFKA_PLAN`, `KAFKA_REGISTER_SCHEMA`, `KAFKA_APPLY` and `KAFKA_ROLLBACK` stages.
- Deploy-target safety rails: `allowTopicDeletion`, `protectedTopics`,
  `maxPartitionDecrease`, `driftDetectionEnabled`.
- Per-OS, per-arch binaries published by the `release` workflow.
