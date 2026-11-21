"""The provisioned Grafana dashboards: valid JSON that only uses provisioned data sources."""

import json
from pathlib import Path

import pytest

GRAFANA = Path(__file__).resolve().parents[1] / "infra" / "grafana"
DASHBOARDS = sorted((GRAFANA / "dashboards").glob("*.json"))
GRID_WIDTH = 24
# Panels that plot numbers, so need a unit; tables and maps format each column themselves.
NUMERIC_PANELS = {"stat", "timeseries", "barchart", "gauge"}


def provisioned_datasources() -> dict[str, str]:
    """uid -> type, read from the provisioning files (simple YAML, parsed by hand)."""
    found, entry = {}, {}
    for path in (GRAFANA / "provisioning" / "datasources").glob("*.yaml"):
        for line in path.read_text().splitlines():
            key, _, value = line.strip().removeprefix("- ").partition(":")
            if key in {"type", "uid"}:
                entry[key] = value.strip()
            if {"type", "uid"} <= entry.keys():
                found[entry["uid"]] = entry["type"]
                entry = {}
    return found


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def test_there_are_dashboards_and_datasources():
    assert DASHBOARDS
    assert provisioned_datasources() == {"prometheus": "prometheus", "trino": "trino-datasource"}


def test_dashboard_uids_are_unique():
    uids = [load(path)["uid"] for path in DASHBOARDS]
    assert len(uids) == len(set(uids))


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.stem)
def test_panels_are_well_formed(path):
    panels = load(path)["panels"]
    ids = [panel["id"] for panel in panels]
    assert len(ids) == len(set(ids)), "panel ids must be unique"
    for panel in panels:
        grid = panel["gridPos"]
        assert grid["x"] + grid["w"] <= GRID_WIDTH, panel["title"]
        assert panel["title"]
        if panel["type"] == "row":
            continue
        assert panel["targets"], f"{panel['title']} has no query"
        refs = [target["refId"] for target in panel["targets"]]
        assert len(refs) == len(set(refs)), panel["title"]
        if panel["type"] in NUMERIC_PANELS:
            assert panel["fieldConfig"]["defaults"].get("unit"), f"{panel['title']} has no unit"


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.stem)
def test_panels_overlap_nowhere(path):
    cells = {}
    for panel in load(path)["panels"]:
        grid = panel["gridPos"]
        for x in range(grid["x"], grid["x"] + grid["w"]):
            for y in range(grid["y"], grid["y"] + grid["h"]):
                assert (x, y) not in cells, f"{panel['title']} overlaps {cells[(x, y)]}"
                cells[(x, y)] = panel["title"]


@pytest.mark.parametrize("path", DASHBOARDS, ids=lambda p: p.stem)
def test_queries_use_provisioned_datasources(path):
    datasources = provisioned_datasources()
    for panel in load(path)["panels"]:
        refs = [panel.get("datasource"), *(t.get("datasource") for t in panel.get("targets", []))]
        for ref in filter(None, refs):
            assert datasources.get(ref["uid"]) == ref["type"], panel["title"]
        for target in panel.get("targets", []):
            query = target.get("rawSQL") or target.get("expr")
            assert query, f"{panel['title']} {target['refId']} has an empty query"
            if target["datasource"]["type"] == "trino-datasource":
                # The plugin has no default catalog, so tables are named in full.
                assert "lakehouse." in query, panel["title"]
