
import dagster as dg
from .defs import assets
from .defs.resources import OuraAPI, DuckDBResource

@dg.definitions
def defs():
    return dg.Definitions(
        assets=dg.load_assets_from_modules([assets]),
        resources={
            "oura_api": OuraAPI(
                client_id=dg.EnvVar("OURA_CLIENT_ID"),
                client_secret=dg.EnvVar("OURA_CLIENT_SECRET"),
                token_path=dg.EnvVar("OURA_TOKEN_PATH"),
            ),
            "duckdb": DuckDBResource(
                db_path=dg.EnvVar("DUCKDB_PATH"),
            ),
        },
        executor=dg.in_process_executor
    )