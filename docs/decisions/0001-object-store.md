# 0001: Object store

- **Status:** Accepted
- **Date:** 2026-11-10

## Context

Iceberg tables need S3-compatible object storage. It has to run locally in Docker Compose on a laptop, be open source under a stable licence, and work with Spark, Trino, PyIceberg and the Polaris catalog.

The plan listed three candidates: MinIO, SeaweedFS and Garage.

## Options

**MinIO.** It used to be the default choice for local S3, but the community edition has been wound down:
- May 2025: the admin console was removed from the community edition.
- October 2025: MinIO stopped publishing community Docker images and binaries.
- December 2025: the community edition went into maintenance mode.
- 2026: the repository was archived.

New builds ship only as AIStor, under a proprietary licence. Using MinIO now means running an unmaintained image or building it from source.

**SeaweedFS.** Apache 2.0 and actively maintained. `weed mini` runs the whole thing in a single container, with S3 on port 8333. Integration guides exist for Spark, Trino, DuckDB, ClickHouse and Doris. It also includes an Iceberg REST catalog, which this project doesn't use.

**Garage.** Lightweight and actively maintained, but AGPL-licensed, with a smaller ecosystem and fewer documented Iceberg integrations.

## Decision

Use **SeaweedFS** as the object store, through its S3 API only.

- Pin version **4.42 or later**. Earlier versions could delete committed objects when a parent folder was removed.
- Keep **Apache Polaris** as the Iceberg catalog, and don't use SeaweedFS's built-in REST catalog. Polaris is engine-neutral, and the catalog is a separate choice (see the planned catalog decision record).

## Consequences

- Everything in the storage layer is Apache-licensed, which matches the plan's licence preference.
- The stack runs one more service than it would with the built-in catalog. That costs some RAM but keeps storage and catalog independent.
- SeaweedFS is less familiar to recruiters than MinIO. This record explains the choice.
- Switching stores later only means changing the S3 endpoint and credentials, because Spark, Trino and Polaris all talk to plain S3.

## References

- [MinIO CE in 2026: Retired Upstream, Source-Only, and What to Use](https://glukhov.org/data-infrastructure/object-storage/minio-dead/)
- [It's FOSS: MinIO moves away from open source](https://itsfoss.com/news/minio-moves-away-from-open-source/)
- [ClickHouse docs: SeaweedFS S3](https://clickhouse.com/docs/integrations/connectors/data-ingestion/s3-seaweedfs)
- [Apache Doris: Doris with SeaweedFS](https://doris.apache.org/docs/3.x/lakehouse/best-practices/doris-seaweedfs/)

MinIO's timeline comes from secondary sources. Check the dates against the official MinIO GitHub repository before citing them.
