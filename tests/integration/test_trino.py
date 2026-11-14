"""Trino on the Polaris catalog: it reads tables other engines write, and its writes are
readable elsewhere (dbt will write gold through it).

Needs the stack and Trino running (make trino). Run with: make test-trino
"""

import uuid

import pyarrow as pa
import pytest

from tests.integration.helpers import host_catalog, purge_tables, trino_cursor

pytestmark = [pytest.mark.integration, pytest.mark.trino]

NAMESPACE = "smoke"


@pytest.fixture
def catalog():
    catalog = host_catalog()
    catalog.create_namespace_if_not_exists(NAMESPACE)
    return catalog


@pytest.fixture
def cursor():
    return trino_cursor()


def test_trino_reads_a_table_written_by_pyiceberg(catalog, cursor):
    table_name = f"{NAMESPACE}.positions_{uuid.uuid4().hex[:8]}"
    rows = pa.table(
        {
            "icao24": pa.array(["3c6444", "4ca7b4"], pa.string()),
            "latitude": pa.array([40.47, 41.29], pa.float64()),
        }
    )
    table = catalog.create_table(table_name, schema=rows.schema)
    try:
        table.append(rows)
        cursor.execute(f"SELECT icao24, latitude FROM {table_name} ORDER BY icao24")  # noqa: S608
        assert cursor.fetchall() == [["3c6444", 40.47], ["4ca7b4", 41.29]]
    finally:
        purge_tables(catalog, table_name)


def test_trino_writes_a_table_readable_by_pyiceberg(catalog, cursor):
    table_name = f"{NAMESPACE}.airports_{uuid.uuid4().hex[:8]}"
    try:
        cursor.execute(
            f"CREATE TABLE {table_name} AS "  # noqa: S608
            "SELECT * FROM (VALUES ('LEBL', 4), ('LEGE', 143)) AS t (ident, elevation_m)"
        )
        cursor.fetchall()
        result = catalog.load_table(table_name).scan().to_arrow().sort_by("ident")
        assert result.to_pylist() == [
            {"ident": "LEBL", "elevation_m": 4},
            {"ident": "LEGE", "elevation_m": 143},
        ]
    finally:
        purge_tables(catalog, table_name)
