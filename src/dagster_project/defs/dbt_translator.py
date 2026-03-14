import dagster as dg
from dagster_dbt import DagsterDbtTranslator

# Simple prefix → group map
_LAYER_MAP = {
    "stg_":  "staging",
    "int_":  "intermediate",
    "tr_":   "intermediate",
    "agg_":  "intermediate",
    "join_": "intermediate",
    "fact_": "marts",
    "dim_":  "marts",
}

class OuraTranslator(DagsterDbtTranslator):
    """
    - Map dbt sources under 'oura_raw' to your raw Dagster assets keyed ["oura_raw", <table>].
    - Group dbt models by name prefix (stg_/int_/tr_/agg_/join_/fact_/dim_).
    """

    # keep your existing source mapping exactly as-is
    def get_asset_key_for_source(self, dbt_source_props):
        source_name = dbt_source_props.get("source_name")
        table_name  = dbt_source_props.get("name")
        if source_name == "oura_raw" and table_name:
            return dg.AssetKey(["oura_raw", table_name])
        return super().get_asset_key_for_source(dbt_source_props)

    # minimal grouping by model name prefix
    def get_group_name(self, dbt_resource_props):
        if dbt_resource_props.get("resource_type") == "model":
            name = dbt_resource_props.get("name", "")
            for prefix, group in _LAYER_MAP.items():
                if name.startswith(prefix):
                    return group
            return "models"  # default bucket for models without a known prefix
        return super().get_group_name(dbt_resource_props)