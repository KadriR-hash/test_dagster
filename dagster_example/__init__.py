"""Definitions that provide Dagster code locations."""
from dagster import Definitions
from dagster_example.jobs import dagster_etl_pipeline

defs = Definitions(
    assets=[],
    jobs=[dagster_etl_pipeline],
    schedules=[],
)
