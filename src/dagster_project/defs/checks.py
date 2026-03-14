"""Asset checks for Oura raw data tables."""

import dagster as dg

from .assets import VALID_TABLES
from .resources import DuckDBResource


def _check_row_count(con, table: str) -> dg.AssetCheckResult:
    """Run a row count check against a DuckDB table. Testable without Dagster context."""
    count = con.execute(f"SELECT COUNT(*) FROM oura_raw.{table}").fetchone()[0]
    return dg.AssetCheckResult(
        passed=count > 0,
        metadata={"row_count": dg.MetadataValue.int(count)},
    )


def make_row_count_check(table: str) -> dg.AssetChecksDefinition:
    """Factory that creates a row-count asset check for a given raw table."""

    @dg.asset_check(
        asset=dg.AssetKey(["oura_raw", table]),
        description=f"Checks that oura_raw.{table} has at least one row.",
    )
    def _check(duckdb: DuckDBResource) -> dg.AssetCheckResult:
        con = duckdb.get_connection()
        result = _check_row_count(con, table)
        con.close()
        return result

    _check.__name__ = f"{table}_has_rows"
    _check.__qualname__ = f"{table}_has_rows"
    return _check


row_count_checks = [make_row_count_check(t) for t in sorted(VALID_TABLES)]
