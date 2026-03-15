"""Tests for _upsert_day() with Snowflake backend."""

from datetime import date

import pytest

from dagster_project.defs.assets import _upsert_day

pytestmark = pytest.mark.skipif(
    "SNOWFLAKE_ACCOUNT" not in __import__("os").environ,
    reason="Snowflake credentials not available",
)


class TestUpsertDayInsert:
    """Basic insert operations."""

    def test_creates_table_and_inserts_rows(self, snowflake_con):
        rows = [{"id": "abc", "score": 85, "day": "2024-06-01"}]
        count = _upsert_day(snowflake_con, "sleep", rows, date(2024, 6, 1))

        assert count == 1
        cursor = snowflake_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM oura_raw.sleep")
        assert cursor.fetchone()[0] == 1

    def test_return_value_matches_row_count(self, snowflake_con):
        rows = [
            {"id": f"r{i}", "bpm": 60 + i, "timestamp": "2024-06-01T00:00:00"}
            for i in range(5)
        ]
        count = _upsert_day(snowflake_con, "heartrate", rows, date(2024, 6, 1))
        assert count == 5

        cursor = snowflake_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM oura_raw.heartrate")
        assert cursor.fetchone()[0] == 5

    def test_raw_data_is_valid_json(self, snowflake_con):
        """Verify raw_data is stored as parseable VARIANT."""
        rows = [{"id": "x1", "score": 90, "day": "2024-06-01"}]
        _upsert_day(snowflake_con, "sleep", rows, date(2024, 6, 1))

        cursor = snowflake_con.cursor()
        cursor.execute(
            "SELECT raw_data:id::varchar, raw_data:score::int FROM oura_raw.sleep"
        )
        row = cursor.fetchone()
        assert row[0] == "x1"
        assert row[1] == 90


class TestUpsertDayEmpty:
    """Empty input handling."""

    def test_empty_list_returns_zero(self, snowflake_con):
        count = _upsert_day(snowflake_con, "sleep", [], date(2024, 6, 1))
        assert count == 0

    def test_empty_creates_table(self, snowflake_con):
        _upsert_day(snowflake_con, "activity", [], date(2024, 6, 1))

        cursor = snowflake_con.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = 'OURA_RAW' AND table_name = 'ACTIVITY'"
        )
        assert cursor.fetchone()[0] == 1

    def test_none_rows_returns_zero(self, snowflake_con):
        count = _upsert_day(snowflake_con, "sleep", None, date(2024, 6, 1))
        assert count == 0


class TestUpsertDayIdempotent:
    """DELETE + INSERT idempotency."""

    def test_same_day_replaces_rows(self, snowflake_con):
        day = date(2024, 6, 1)
        _upsert_day(snowflake_con, "sleep", [{"id": "old"}], day)
        _upsert_day(snowflake_con, "sleep", [{"id": "new"}], day)

        cursor = snowflake_con.cursor()
        cursor.execute("SELECT raw_data:id::varchar FROM oura_raw.sleep")
        ids = [r[0] for r in cursor.fetchall()]
        assert ids == ["new"]

    def test_different_days_coexist(self, snowflake_con):
        _upsert_day(snowflake_con, "sleep", [{"id": "d1"}], date(2024, 6, 1))
        _upsert_day(snowflake_con, "sleep", [{"id": "d2"}], date(2024, 6, 2))

        cursor = snowflake_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM oura_raw.sleep")
        assert cursor.fetchone()[0] == 2

    def test_rerun_does_not_duplicate(self, snowflake_con):
        day = date(2024, 6, 1)
        row = [{"id": "same"}]
        _upsert_day(snowflake_con, "sleep", row, day)
        _upsert_day(snowflake_con, "sleep", row, day)
        _upsert_day(snowflake_con, "sleep", row, day)

        cursor = snowflake_con.cursor()
        cursor.execute("SELECT COUNT(*) FROM oura_raw.sleep")
        assert cursor.fetchone()[0] == 1


class TestUpsertDayPartitionDate:
    """Partition date injection."""

    def test_adds_partition_date(self, snowflake_con):
        day = date(2024, 6, 15)
        _upsert_day(snowflake_con, "sleep", [{"id": "p1"}], day)

        cursor = snowflake_con.cursor()
        cursor.execute("SELECT partition_date FROM oura_raw.sleep")
        assert cursor.fetchone()[0] == day


class TestUpsertDayInvalidTable:
    """Table name validation (SQL injection guard)."""

    def test_invalid_table_raises(self, snowflake_con):
        with pytest.raises(ValueError, match="Invalid table name"):
            _upsert_day(snowflake_con, "not_a_table", [{"x": 1}], date(2024, 6, 1))

    def test_sql_injection_attempt(self, snowflake_con):
        with pytest.raises(ValueError, match="Invalid table name"):
            _upsert_day(
                snowflake_con,
                "sleep; DROP TABLE oura_raw.sleep; --",
                [{"x": 1}],
                date(2024, 6, 1),
            )
