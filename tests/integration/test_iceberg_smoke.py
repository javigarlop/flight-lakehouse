"""Smoke test: create, write and read an Iceberg table through Polaris and SeaweedFS.

Needs the stack running (make up). Run with: make smoke
"""

import uuid

import pyarrow as pa
import pytest

from tests.integration.helpers import host_catalog

pytestmark = pytest.mark.integration


@pytest.fixture
def catalog():
    return host_catalog()


def test_create_write_read_table(catalog):
    namespace = "smoke"
    table_name = f"{namespace}.positions_{uuid.uuid4().hex[:8]}"
    catalog.create_namespace_if_not_exists(namespace)

    rows = pa.table(
        {
            "icao24": pa.array(["3c6444", "4ca7b4"], pa.string()),
            "latitude": pa.array([40.47, 41.29], pa.float64()),
            "longitude": pa.array([-3.56, 2.07], pa.float64()),
        }
    )
    table = catalog.create_table(table_name, schema=rows.schema)
    try:
        table.append(rows)
        result = catalog.load_table(table_name).scan().to_arrow().sort_by("icao24")
        assert result.equals(rows)
    finally:
        catalog.drop_table(table_name)
