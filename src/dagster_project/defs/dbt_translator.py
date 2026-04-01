from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster_dbt import DagsterDbtTranslator

# Simple prefix → group map
_LAYER_MAP = {
    "stg_": "staging",
    "int_": "intermediate",
    "tr_": "intermediate",
    "agg_": "intermediate",
    "join_": "intermediate",
    "fact_": "marts",
    "dim_": "marts",
}


class OuraTranslator(DagsterDbtTranslator):
    """Map dbt sources and models to Dagster asset keys and groups.

    - Maps dbt sources under ``oura_raw`` to raw Dagster assets
      keyed ``["oura_raw", <table>]``.
    - Groups dbt models by name prefix
      (stg_/int_/tr_/agg_/join_/fact_/dim_).
    """

    def get_asset_key_for_source(
        self, dbt_source_props: Mapping[str, Any]
    ) -> dg.AssetKey:
        """Map a dbt source to a Dagster AssetKey.

        Parameters
        ----------
        dbt_source_props : Mapping[str, Any]
            Properties dict for the dbt source node, containing at
            minimum ``source_name`` and ``name`` keys.

        Returns
        -------
        dg.AssetKey
            ``["oura_raw", <table_name>]`` for oura_raw sources,
            otherwise delegates to the parent translator.
        """
        source_name = dbt_source_props.get("source_name")
        table_name = dbt_source_props.get("name")
        if source_name == "oura_raw" and table_name:
            return dg.AssetKey(["oura_raw", table_name])
        return super().get_asset_key_for_source(dbt_source_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        """Assign a Dagster group based on dbt model name prefix.

        Parameters
        ----------
        dbt_resource_props : Mapping[str, Any]
            Properties dict for the dbt resource node.

        Returns
        -------
        str | None
            Group name derived from the model name prefix, ``"models"``
            for models without a known prefix, or the parent's group
            for non-model resources.
        """
        if dbt_resource_props.get("resource_type") == "model":
            name = dbt_resource_props.get("name", "")
            for prefix, group in _LAYER_MAP.items():
                if name.startswith(prefix):
                    return group
            return "models"  # default bucket for models without a known prefix
        return super().get_group_name(dbt_resource_props)
