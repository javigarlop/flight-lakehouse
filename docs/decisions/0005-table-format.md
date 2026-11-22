# 0005: Table format

- **Status:** Accepted
- **Date:** 2026-11-22 (written up after Phase 4; the choice was made in Phase 0)

## Context

Bronze, silver and gold live as tables on object storage (SeaweedFS, see
[0001](0001-object-store.md)). Three engines use them: Spark writes bronze and silver and
runs maintenance, Trino serves queries and runs dbt for gold, and PyIceberg reads them in
tests. The format needs ACID commits for streaming writes, schema evolution, time-based
retention, and a catalog that isn't tied to one engine or vendor.

## Options

**Apache Iceberg.** An open specification with implementations in Spark, Trino, Flink,
PyIceberg and others. Hidden partitioning: tables are partitioned by a transform of a column,
such as `days(event_time)`, and queries that filter on the column are pruned without knowing
the layout. Partitioning can change later without rewriting data. A REST catalog
specification, implemented by several open source catalogs, lets every engine share one
catalog.

**Delta Lake.** Open source and mature, with its strongest support in Spark. Trino has a
Delta connector. Partitioning uses explicit columns, so a daily partition needs a derived
`date` column that queries have to filter on. The open catalog options are fewer, and the
best-integrated one is tied to the Databricks ecosystem.

**Apache Hudi.** Strong at upserts and incremental pulls, but more complex to run, and this
pipeline is append-mostly.

## Decision

Use **Apache Iceberg** (format version 2) for every table, through the Polaris REST catalog.

## Consequences

- Spark, Trino and PyIceberg read and write the same tables through one catalog, with no
  copy or conversion between engines.
- Schema evolution is a metadata change. Schema v2's `emergency` column was added to bronze
  and silver while the pipeline ran, with no rewrite (`make schema-demo`).
- Retention drops whole day partitions, which only changes metadata (see
  [0006](0006-partitioning.md)).
- Maintenance is part of running Iceberg: compaction, snapshot expiry and orphan cleanup
  run daily, or the small streaming files and old snapshots pile up.
- Delta's tighter Spark integration, such as `OPTIMIZE` with Z-ordering, is not available
  in the same form. Iceberg has sort-based compaction instead, and this data is too small to
  need either.

## References

- [Iceberg table spec](https://iceberg.apache.org/spec/)
- [Iceberg: Partitioning](https://iceberg.apache.org/docs/latest/partitioning/)
- [Trino: Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [Delta Lake](https://delta.io/)
