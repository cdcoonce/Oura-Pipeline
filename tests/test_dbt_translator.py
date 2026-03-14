import pytest
import dagster as dg
from dagster_project.defs.dbt_translator import OuraTranslator


@pytest.fixture
def translator():
    return OuraTranslator()


# --- get_asset_key_for_source ---


class TestGetAssetKeyForSource:
    def test_oura_raw_source_returns_correct_asset_key(self, translator):
        props = {"source_name": "oura_raw", "name": "sleep"}
        result = translator.get_asset_key_for_source(props)
        assert result == dg.AssetKey(["oura_raw", "sleep"])

    def test_oura_raw_source_different_table(self, translator):
        props = {"source_name": "oura_raw", "name": "heartrate"}
        result = translator.get_asset_key_for_source(props)
        assert result == dg.AssetKey(["oura_raw", "heartrate"])

    def test_non_oura_source_delegates_to_parent(self, translator):
        """Non-oura_raw sources delegate to parent, which raises AttributeError."""
        props = {
            "source_name": "other_source",
            "name": "some_table",
        }
        with pytest.raises(AttributeError):
            translator.get_asset_key_for_source(props)


# --- get_group_name ---


class TestGetGroupName:
    @pytest.mark.parametrize(
        "prefix, expected_group",
        [
            ("stg_", "staging"),
            ("int_", "intermediate"),
            ("tr_", "intermediate"),
            ("agg_", "intermediate"),
            ("join_", "intermediate"),
            ("fact_", "marts"),
            ("dim_", "marts"),
        ],
    )
    def test_model_prefix_returns_correct_group(
        self, translator, prefix, expected_group
    ):
        props = {"resource_type": "model", "name": f"{prefix}example"}
        assert translator.get_group_name(props) == expected_group

    def test_model_without_known_prefix_returns_models(self, translator):
        props = {"resource_type": "model", "name": "some_random_model"}
        assert translator.get_group_name(props) == "models"

    def test_model_with_empty_name_returns_models(self, translator):
        props = {"resource_type": "model", "name": ""}
        assert translator.get_group_name(props) == "models"

    def test_non_model_resource_delegates_to_parent(self, translator):
        """Non-model resources delegate to parent, which returns None."""
        props = {
            "resource_type": "seed",
            "name": "stg_something",
        }
        result = translator.get_group_name(props)
        assert result is None
