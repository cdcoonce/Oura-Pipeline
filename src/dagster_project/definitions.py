import dagster as dg
from dagster_dbt import DbtCliResource

from .defs import assets, checks, dbt_assets, report_assets, schedules
from .defs.resources import DuckDBResource, OuraAPI
from .reports.report_delivery import SESDeliveryResource


@dg.definitions
def defs():
    return dg.load_definitions_from_modules(
        modules=[assets, checks, dbt_assets, report_assets, schedules],
        resources={
            "oura_api": OuraAPI(
                client_id=dg.EnvVar("OURA_CLIENT_ID"),
                client_secret=dg.EnvVar("OURA_CLIENT_SECRET"),
                token_path=dg.EnvVar("OURA_TOKEN_PATH"),
            ),
            "duckdb": DuckDBResource(
                db_path=dg.EnvVar("DUCKDB_PATH"),
            ),
            "dbt": DbtCliResource(
                project_dir=dbt_assets.DBT_PROJECT_DIR,
                profiles_dir=dbt_assets.DBT_PROFILES_DIR,
                target="dev",
            ),
            "ses": SESDeliveryResource(
                sender_email=dg.EnvVar("SES_SENDER_EMAIL"),
                recipient_email=dg.EnvVar("SES_RECIPIENT_EMAIL"),
                aws_region=dg.EnvVar("AWS_REGION"),
            ),
        },
        executor=dg.in_process_executor,
    )
