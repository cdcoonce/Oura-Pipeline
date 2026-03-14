import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from .dbt_translator import OuraTranslator

DBT_PROJECT_DIR = "dbt_oura"
DBT_PROFILES_DIR = "dbt_oura"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)
dbt_project.prepare_if_dev()

dbt_manifest = dbt_project.manifest_path


@dbt_assets(manifest=dbt_manifest, dagster_dbt_translator=OuraTranslator())
def dbt_model_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build", "--no-use-colors"], context=context).stream()
