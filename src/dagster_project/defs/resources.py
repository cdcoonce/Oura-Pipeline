import dagster as dg
import duckdb
import os
import pathlib
from dotenv import load_dotenv
from oura_client.client import OuraClient

@dg.resource
def oura_client():
    return OuraClient()

@dg.resource
def duckdb_conn():
    load_dotenv()
    db_path = os.environ.get("DUCKDB_PATH", "data/oura.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS oura_raw;")
    return conn

@dg.resource
def data_dir():
    p = pathlib.Path("data/raw")
    p.mkdir(parents=True, exist_ok=True)
    return p