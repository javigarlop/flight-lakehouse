# 0003: Dashboard

- **Status:** Accepted
- **Date:** 2026-11-12

## Context

The gold tables (airport traffic, route stats, holding patterns, pipeline freshness) need a dashboard. It is the one view that shows the pipeline working to someone who doesn't read the code. Phase 3 ends when "the dashboard shows live data", and the README needs a screenshot.

Phase 4 already adds Grafana for pipeline metrics: consumer lag, throughput and freshness. The stack is the plan's main resource risk (about 12–16 GB of RAM), so every extra service has a cost.

The plan listed two candidates, Superset and Evidence. Grafana was added once it was clear it could read the gold tables too.

## Options

**Grafana with the Trino data source.** The `trino-datasource` plugin is maintained by the Trino project (`trinodb/grafana-trino`) and listed in Grafana's plugin catalog. Current releases need Grafana 10.4 or later. One service then shows both the gold tables and the pipeline metrics, and dashboards can be kept as code in `infra/grafana/`. It is strongest for time series. Business-style charts and maps are weaker than in Superset.

**Superset.** The best-known open source BI tool, with good maps and charts. It needs its own metadata database and, for async queries and caching, Redis and worker processes. That is the heaviest of the three options.

**Evidence.** Dashboards are SQL plus Markdown in git, which suits a code repo, and it supports Trino. It builds a static site rather than a live view, and it would be one more tool alongside Grafana.

## Decision

Use **Grafana** for the gold dashboard, through the Trino data source.

- Bring Grafana forward from Phase 4 to Phase 3, with the Trino plugin pinned.
- Keep dashboards as code (JSON in `infra/grafana/`), provisioned on start.
- Revisit Evidence only if the Grafana screenshot reads too much like an ops dashboard for the README.

## Consequences

- One fewer service than adding Superset or Evidence; the Phase 4 metrics dashboards reuse the same Grafana.
- Grafana is AGPL-3.0, not Apache-licensed. It is still open source and was already in the plan, so this adds no new licence.
- Map-style views (such as holding patterns around an airport) are limited to Grafana's geomap panel. That is enough for points and routes, but not for rich GIS views.
- Switching later is cheap: gold lives in Iceberg and is read through Trino, so any tool with a Trino connector can replace Grafana.

## References

- [Grafana plugin catalog: Trino](https://grafana.com/grafana/plugins/trino-datasource/)
- [trinodb/grafana-trino on GitHub](https://github.com/trinodb/grafana-trino)
- [Evidence docs: Trino data source](https://docs.evidence.dev/core-concepts/data-sources/trino)
