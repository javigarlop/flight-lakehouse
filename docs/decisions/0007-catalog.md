# 0007: Iceberg catalog

- **Status:** Accepted
- **Date:** 2026-11-22 (written up after Phase 4; the choice was made in Phase 0)

## Context

An Iceberg catalog maps table names to their current metadata and makes commits atomic.
Spark, Trino and PyIceberg all need to use the same one. It has to be open source, run
locally in Docker Compose, keep its state across restarts, and work with SeaweedFS
([0001](0001-object-store.md)), which has no AWS STS for short-lived credentials.

## Options

- **Apache Polaris.** An Iceberg REST catalog, originally from Snowflake, now an Apache
  project. Engine-neutral, with OAuth2 clients and role-based access. Its default store is in
  memory; it can keep state in Postgres.
- **Project Nessie.** A REST catalog with Git-like branches and tags across tables. Powerful,
  but this project doesn't need branching.
- **Lakekeeper.** A lightweight REST catalog written in Rust, Apache-licensed. A younger
  project with a smaller community.
- **SeaweedFS's built-in REST catalog.** One service fewer, but it ties the catalog to the
  object store.
- **Hive Metastore or Iceberg's JDBC catalog.** Hive Metastore is heavy and older. The JDBC
  catalog is simple, but every engine then needs database access, and some engines support
  it poorly.

## Decision

Use **Apache Polaris 1.8** as the Iceberg REST catalog, with its state in **Postgres**.

- Spark and Trino authenticate with OAuth2 client credentials (`POLARIS_CLIENT_ID` and
  `POLARIS_CLIENT_SECRET`).
- Credential vending is off. SeaweedFS can't issue temporary credentials, so each engine uses
  its own S3 keys.
- `infra/compose/polaris/init.sh` creates the catalog on start and is safe to re-run.

## Consequences

- Any engine that speaks the Iceberg REST protocol can use the tables, so the catalog can be
  replaced without moving data.
- Tables survive restarts, because Polaris's metadata is in Postgres, not memory.
- Two more services to run: Polaris and its Postgres.
- Without credential vending, every engine holds the S3 keys directly. That's acceptable on a
  single machine; a shared deployment would want a store with STS.

## References

- [Apache Polaris](https://polaris.apache.org/)
- [Iceberg REST catalog specification](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- [Project Nessie](https://projectnessie.org/)
- [Lakekeeper](https://docs.lakekeeper.io/)
