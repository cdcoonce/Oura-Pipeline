"""Tests for DuckDBResource shared connection behavior."""

import duckdb
import pytest

from dagster_project.defs.resources import DuckDBResource


@pytest.fixture
def tmp_db(tmp_path):
    """Return a path to a temporary DuckDB database file."""
    return str(tmp_path / "test.duckdb")


class TestDuckDBResourceSharedConnection:
    """DuckDBResource should reuse a single connection to avoid file-lock conflicts."""

    def test_returns_same_connection_on_successive_calls(self, tmp_db):
        resource = DuckDBResource(db_path=tmp_db)
        con1 = resource.get_connection()
        con2 = resource.get_connection()
        assert con1 is con2

    def test_connection_is_valid_duckdb_connection(self, tmp_db):
        resource = DuckDBResource(db_path=tmp_db)
        con = resource.get_connection()
        assert isinstance(con, duckdb.DuckDBPyConnection)

    def test_creates_oura_raw_schema(self, tmp_db):
        resource = DuckDBResource(db_path=tmp_db)
        con = resource.get_connection()
        schemas = con.execute(
            "SELECT schema_name FROM information_schema.schemata"
        ).fetchall()
        schema_names = [row[0] for row in schemas]
        assert "oura_raw" in schema_names

    def test_close_resets_cached_connection(self, tmp_db):
        resource = DuckDBResource(db_path=tmp_db)
        con1 = resource.get_connection()
        resource.close()
        con2 = resource.get_connection()
        assert con1 is not con2

    def test_get_connection_after_close_works(self, tmp_db):
        resource = DuckDBResource(db_path=tmp_db)
        resource.get_connection()
        resource.close()
        con = resource.get_connection()
        result = con.execute("SELECT 1").fetchone()
        assert result == (1,)
