import duckdb
import pytest
from datetime import date

from dagster_project.defs.assets import _upsert_day, VALID_TABLES


@pytest.fixture()
def con():
    """In-memory DuckDB connection, closed after each test."""
    connection = duckdb.connect()
    yield connection
    connection.close()


DAY = date(2025, 3, 10)


class TestUpsertDayInsert:
    """Inserting rows creates the table and inserts data correctly."""

    def test_creates_table_and_inserts_rows(self, con):
        rows = [
            {"id": "abc", "score": 80},
            {"id": "def", "score": 92},
        ]
        count = _upsert_day(con, "sleep", rows, DAY)

        assert count == 2
        result = con.execute("SELECT * FROM oura_raw.sleep ORDER BY id").fetchall()
        assert len(result) == 2
        assert result[0][0] == "abc"
        assert result[1][0] == "def"

    def test_return_value_matches_row_count(self, con):
        rows = [{"id": str(i)} for i in range(5)]
        count = _upsert_day(con, "activity", rows, DAY)
        assert count == 5


class TestUpsertDayEmpty:
    """Empty rows returns 0 and creates an empty table stub."""

    def test_empty_list_returns_zero(self, con):
        count = _upsert_day(con, "sleep", [], DAY)
        assert count == 0

    def test_empty_list_creates_table_with_full_schema(self, con):
        _upsert_day(con, "sleep", [], DAY)
        cols = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'oura_raw' AND table_name = 'sleep'"
        ).fetchall()
        col_names = [c[0] for c in cols]
        assert "partition_date" in col_names
        assert "id" in col_names
        assert "score" in col_names
        assert "contributors" in col_names

    def test_empty_table_has_no_rows(self, con):
        _upsert_day(con, "sleep", [], DAY)
        result = con.execute("SELECT COUNT(*) FROM oura_raw.sleep").fetchone()
        assert result[0] == 0


class TestUpsertDayIdempotent:
    """Re-running for the same day replaces data, not duplicates."""

    def test_same_day_replaces_rows(self, con):
        rows_v1 = [{"id": "abc", "score": 80}]
        rows_v2 = [{"id": "abc", "score": 99}]

        _upsert_day(con, "sleep", rows_v1, DAY)
        _upsert_day(con, "sleep", rows_v2, DAY)

        result = con.execute("SELECT * FROM oura_raw.sleep").fetchall()
        assert len(result) == 1
        assert result[0][1] == 99  # score updated

    def test_different_days_coexist(self, con):
        day1 = date(2025, 3, 10)
        day2 = date(2025, 3, 11)

        _upsert_day(con, "sleep", [{"id": "a"}], day1)
        _upsert_day(con, "sleep", [{"id": "b"}], day2)

        result = con.execute("SELECT COUNT(*) FROM oura_raw.sleep").fetchone()
        assert result[0] == 2

    def test_rerun_does_not_duplicate(self, con):
        rows = [{"id": "x", "value": 1}]
        _upsert_day(con, "sleep", rows, DAY)
        _upsert_day(con, "sleep", rows, DAY)
        _upsert_day(con, "sleep", rows, DAY)

        result = con.execute("SELECT COUNT(*) FROM oura_raw.sleep").fetchone()
        assert result[0] == 1


class TestUpsertDayPartitionDate:
    """partition_date column is automatically added if not present."""

    def test_adds_partition_date_column(self, con):
        rows = [{"id": "abc", "score": 80}]
        _upsert_day(con, "sleep", rows, DAY)

        result = con.execute("SELECT partition_date FROM oura_raw.sleep").fetchone()
        assert result[0] == DAY

    def test_preserves_existing_partition_date(self, con):
        explicit_day = date(2025, 1, 1)
        rows = [{"id": "abc", "partition_date": explicit_day}]
        _upsert_day(con, "sleep", rows, DAY)

        result = con.execute("SELECT partition_date FROM oura_raw.sleep").fetchone()
        assert result[0] == explicit_day


class TestUpsertDayInvalidTable:
    """Invalid table name raises ValueError."""

    def test_invalid_table_raises_value_error(self, con):
        with pytest.raises(ValueError, match="Invalid table name"):
            _upsert_day(con, "not_a_table", [{"id": 1}], DAY)

    def test_sql_injection_attempt_raises(self, con):
        with pytest.raises(ValueError, match="Invalid table name"):
            _upsert_day(con, "sleep; DROP TABLE oura_raw.sleep", [], DAY)

    def test_all_valid_tables_accepted(self, con):
        """Smoke test: every table in VALID_TABLES is accepted without error."""
        for table in VALID_TABLES:
            _upsert_day(con, table, [], DAY)
