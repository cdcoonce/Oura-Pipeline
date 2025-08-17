# src/dagster_project/defs/assets.py
import dagster as dg, polars as pl
from datetime import datetime
from pathlib import Path

partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")

def _dates(ctx):
    d = datetime.fromisoformat(ctx.partition_key).date()
    return d, d

@dg.asset(partitions_def=partitions, group_name="oura_raw", required_resource_keys={"oura_client", "data_dir"})
def oura_sleep_raw(context: dg.AssetExecutionContext):
    client = context.resources.oura_client
    data_dir: Path = context.resources.data_dir
    start, end = _dates(context)
    rows = list(client.fetch_daily("sleep", start, end))
    path = data_dir / f"sleep_{start}.parquet"
    (pl.from_dicts(rows) if rows else pl.DataFrame()).write_parquet(path)
    return str(path)

@dg.asset(partitions_def=partitions, group_name="oura_raw", required_resource_keys={"oura_client", "data_dir"})
def oura_readiness_raw(context: dg.AssetExecutionContext):
    client = context.resources.oura_client
    data_dir: Path = context.resources.data_dir
    start, end = _dates(context)
    rows = list(client.fetch_daily("readiness", start, end))
    path = data_dir / f"readiness_{start}.parquet"
    (pl.from_dicts(rows) if rows else pl.DataFrame()).write_parquet(path)
    return str(path)

@dg.asset(partitions_def=partitions, group_name="oura_raw", required_resource_keys={"oura_client", "data_dir"})
def oura_activity_raw(context: dg.AssetExecutionContext):
    client = context.resources.oura_client
    data_dir: Path = context.resources.data_dir
    start, end = _dates(context)
    rows = list(client.fetch_daily("activity", start, end))
    path = data_dir / f"activity_{start}.parquet"
    (pl.from_dicts(rows) if rows else pl.DataFrame()).write_parquet(path)
    return str(path)

@dg.asset(partitions_def=partitions, group_name="oura_raw", required_resource_keys={"oura_client", "data_dir"})
def oura_heartrate_raw(context: dg.AssetExecutionContext):
    client = context.resources.oura_client
    data_dir: Path = context.resources.data_dir
    start, end = _dates(context)
    rows = list(client.fetch_heartrate(start, end))
    path = data_dir / f"heartrate_{start}.parquet"
    (pl.from_dicts(rows) if rows else pl.DataFrame()).write_parquet(path)
    return str(path)

@dg.asset(
    partitions_def=partitions,
    deps=[oura_sleep_raw, oura_readiness_raw, oura_activity_raw, oura_heartrate_raw],
    group_name="staging",
    required_resource_keys={"duckdb_conn", "data_dir"},
)
def load_into_duckdb(context: dg.AssetExecutionContext):
    duckdb_conn = context.resources.duckdb_conn
    data_dir: Path = context.resources.data_dir
    import polars as pl

    # Build paths for the SAME partition key
    day = datetime.fromisoformat(context.partition_key).date()
    paths = {
        "sleep":     data_dir / f"sleep_{day}.parquet",
        "readiness": data_dir / f"readiness_{day}.parquet",
        "activity":  data_dir / f"activity_{day}.parquet",
        "heartrate": data_dir / f"heartrate_{day}.parquet",
    }

    def append(path: Path, table: str):
        if not path.exists():
            return
        df = pl.read_parquet(path)
        if df.is_empty():
            return
        duckdb_conn.register("tmp_df", df.to_pandas())
        duckdb_conn.execute(f"CREATE TABLE IF NOT EXISTS oura_raw.{table} AS SELECT * FROM tmp_df WHERE 1=0;")
        duckdb_conn.execute(f"INSERT INTO oura_raw.{table} SELECT * FROM tmp_df;")
        duckdb_conn.unregister("tmp_df")

    append(paths["sleep"], "sleep")
    append(paths["readiness"], "readiness")
    append(paths["activity"], "activity")
    append(paths["heartrate"], "heartrate")

    return "loaded"