from dagster import multi_asset, AssetSpec, MaterializeResult, AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.loading import snowflake_loader

DBT_PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "dbt")
dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)

@multi_asset(
    specs=[
        AssetSpec(key=["openledger", "transactions"], group_name="ingestion"),
        AssetSpec(key=["openledger", "ledger_entries"], group_name="ingestion"),
        AssetSpec(key=["openledger", "settlements"], group_name="ingestion"),
    ],
    compute_kind="python"
)
def raw_data_ingestion():
    """
    Generates synthetic financial data and loads it into Snowflake.
    Produces the raw source tables for dbt.
    """
    snowflake_loader.main()

    yield MaterializeResult(asset_key=["openledger", "transactions"])
    yield MaterializeResult(asset_key=["openledger", "ledger_entries"])
    yield MaterializeResult(asset_key=["openledger", "settlements"])
    
    return MaterializeResult(metadata={"info": "Ingestion Complete"})

@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()