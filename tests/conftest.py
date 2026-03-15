"""Shared test fixtures for Snowflake integration tests."""

import base64
import os

import pytest
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    load_pem_private_key,
)

SNOWFLAKE_AVAILABLE = "SNOWFLAKE_ACCOUNT" in os.environ


def _get_private_key_bytes() -> bytes:
    """Decode base64 PEM → DER bytes."""
    pem_bytes = base64.b64decode(os.environ["SNOWFLAKE_PRIVATE_KEY"])
    pk = load_pem_private_key(pem_bytes, password=None)
    return pk.private_bytes(Encoding.DER, PrivateFormat.PKCS8, NoEncryption())


@pytest.fixture()
def snowflake_con():
    """Snowflake connection to OURA_TEST.OURA_RAW. Skipped if no credentials."""
    if not SNOWFLAKE_AVAILABLE:
        pytest.skip("Snowflake credentials not available")

    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_get_private_key_bytes(),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="OURA_TEST",
        schema="OURA_RAW",
        role=os.environ.get("SNOWFLAKE_ROLE", "TRANSFORM"),
    )
    yield con

    # Cleanup: drop any tables created during the test
    cursor = con.cursor()
    cursor.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'OURA_RAW' AND table_catalog = 'OURA_TEST'"
    )
    for (table_name,) in cursor.fetchall():
        cursor.execute(f"DROP TABLE IF EXISTS oura_raw.{table_name}")
    con.close()
