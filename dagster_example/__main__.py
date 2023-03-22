"""Example of how to run a Dagster op from normal Python script."""

from dagster_example.jobs import dagster_etl_pipeline

if __name__ == "__main__":
    result = dagster_etl_pipeline.execute_in_process()
