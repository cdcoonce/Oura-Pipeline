import base64
import os
import tempfile

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from .dbt_translator import OuraTranslator

DBT_PROJECT_DIR = "dbt_oura"
DBT_PROFILES_DIR = "dbt_oura"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)
dbt_project.prepare_if_dev()

dbt_manifest = dbt_project.manifest_path


def _ensure_key_file() -> None:
    """Decode base64 PEM env var to a temp .p8 file for dbt-snowflake.

    SNOWFLAKE_PRIVATE_KEY stores base64-encoded PEM, but dbt-snowflake's
    private_key_path needs a file on disk. Writes once per process and
    sets SNOWFLAKE_PRIVATE_KEY_PATH for profiles.yml to reference.
    """
    if os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"):
        return

    b64_key = os.environ.get("SNOWFLAKE_PRIVATE_KEY", "")
    if not b64_key:
        return

    pem_bytes = base64.b64decode(b64_key)
    fd, path = tempfile.mkstemp(suffix=".p8")
    os.write(fd, pem_bytes)
    os.close(fd)
    os.chmod(path, 0o600)
    os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"] = path


@dbt_assets(manifest=dbt_manifest, dagster_dbt_translator=OuraTranslator())
def dbt_model_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    _ensure_key_file()
    yield from dbt.cli(["build", "--no-use-colors"], context=context).stream()
