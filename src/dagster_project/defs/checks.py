"""Asset checks for Oura raw data tables."""

import dagster as dg
import snowflake.connector

from .assets import VALID_TABLES
from .resources import SnowflakeResource


def _check_row_count(
    con: snowflake.connector.SnowflakeConnection,
    table: str,
) -> dg.AssetCheckResult:
    """Run a row count check against a Snowflake table."""
    cursor = con.cursor()

    # Snowflake stores unquoted identifiers in UPPERCASE
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'OURA_RAW' AND table_name = %s",
        (table.upper(),),
    )
    exists = cursor.fetchone()[0] > 0

    if not exists:
        return dg.AssetCheckResult(
            passed=True,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={"row_count": dg.MetadataValue.int(0)},
            description=f"Table oura_raw.{table} does not exist yet.",
        )

    cursor.execute(f"SELECT COUNT(*) FROM oura_raw.{table}")
    count = cursor.fetchone()[0]

    if count == 0:
        return dg.AssetCheckResult(
            passed=True,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={"row_count": dg.MetadataValue.int(count)},
            description=f"Table oura_raw.{table} exists but has no rows.",
        )

    return dg.AssetCheckResult(
        passed=True,
        metadata={"row_count": dg.MetadataValue.int(count)},
    )


def make_row_count_check(table: str) -> dg.AssetChecksDefinition:
    """Factory that creates a row-count asset check for a given raw table."""

    @dg.asset_check(
        asset=dg.AssetKey(["oura_raw", table]),
        description=f"Checks that oura_raw.{table} has at least one row.",
    )
    def _check(snowflake: SnowflakeResource) -> dg.AssetCheckResult:
        con = snowflake.get_connection()
        result = _check_row_count(con, table)
        con.close()
        return result

    _check.__name__ = f"{table}_has_rows"
    _check.__qualname__ = f"{table}_has_rows"
    return _check


row_count_checks = [make_row_count_check(t) for t in sorted(VALID_TABLES)]
