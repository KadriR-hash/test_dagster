"""Collection of Cereal jobs"""
import logging
from datetime import datetime

from dagster import job
import psutil
from dagster_example.ops.cereal import (
    transform,
    extract,
    load,
)

"""@job
def hello_cereal_job():
    
    hello_cereal()


@job
def complex_job():
    
    cereals = download_cereals()
    display_results(
        most_calories=find_highest_calorie_cereal(cereals),
        most_protein=find_highest_protein_cereal(cereals),
    )"""


@job
def dagster_etl_pipeline():
    users = extract()
    df_users = transform(users)
    load(df_users)
