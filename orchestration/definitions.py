from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource
from . import assets

# Loading assets
all_assets = load_assets_from_modules([assets])

# Definitions
defs = Definitions(
    assets=all_assets,
    resources={
        "dbt": DbtCliResource(project_dir=assets.DBT_PROJECT_DIR),
    },
)