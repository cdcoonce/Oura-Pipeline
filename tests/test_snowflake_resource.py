"""Tests for SnowflakeResource."""

import os

import pytest

from dagster_project.defs.resources import SnowflakeResource

pytestmark = pytest.mark.skipif(
    "SNOWFLAKE_ACCOUNT" not in os.environ,
    reason="Snowflake credentials not available",
)


class TestSnowflakeResource:
    def test_get_connection_returns_open_connection(self):
        resource = SnowflakeResource(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key=os.environ["SNOWFLAKE_PRIVATE_KEY"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "OURA"),
            role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORM"),
        )
        con = resource.get_connection()
        try:
            cursor = con.cursor()
            cursor.execute("SELECT CURRENT_ROLE()")
            role = cursor.fetchone()[0]
            assert role == "TRANSFORM"
        finally:
            con.close()

    def test_get_connection_sets_database(self):
        resource = SnowflakeResource(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key=os.environ["SNOWFLAKE_PRIVATE_KEY"],
            database="OURA_TEST",
        )
        con = resource.get_connection()
        try:
            cursor = con.cursor()
            cursor.execute("SELECT CURRENT_DATABASE()")
            assert cursor.fetchone()[0] == "OURA_TEST"
        finally:
            con.close()

    def test_invalid_private_key_raises(self):
        resource = SnowflakeResource(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key="not-valid-base64-pem",
        )
        with pytest.raises(Exception):
            resource.get_connection()
