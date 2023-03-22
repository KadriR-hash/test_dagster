"""Collection of Cereal ops"""
import csv
import logging
import os
from typing import List
import pandas as pd
import json
import requests
from dagster import Nothing, get_dagster_logger, op
from dotenv import load_dotenv
import psutil

logging.basicConfig(level=logging.INFO)
CEREAL_URL = "https://docs.dagster.io/assets/cereal.csv"
load_dotenv()

"""@op
def hello_cereal() -> List[dict]:
    
    response = requests.get(CEREAL_URL, timeout=30)
    lines = response.text.split("\n")
    cereals = list(csv.DictReader(lines))
    get_dagster_logger().info(f"Found {len(cereals)} cereals")
    return cereals


@op
def download_cereals() -> List[dict]:
    
    response = requests.get(CEREAL_URL, timeout=30)
    lines = response.text.split("\n")
    return list(csv.DictReader(lines))


@op
def find_highest_calorie_cereal(cereals: List[dict]) -> str:
    
    sorted_by_calorie = list(sorted(cereals, key=lambda cereal: cereal["calories"]))
    get_dagster_logger().info(
        f'{sorted_by_calorie[-1]["name"]} is the cereal that contains the most calories'
    )
    return sorted_by_calorie[-1]["name"]


@op
def find_highest_protein_cereal(cereals: List[dict]) -> str:
    
    sorted_by_protein = list(sorted(cereals, key=lambda cereal: cereal["protein"]))
    get_dagster_logger().info(
        f'{sorted_by_protein[-1]["name"]} is the cereal that contains the most protein'
    )
    return sorted_by_protein[-1]["name"]


@op
def display_results(most_calories: str, most_protein: str) -> Nothing:
    
    logger = get_dagster_logger()
    logger.info(f"Most caloric cereal: {most_calories}")
    logger.info(f"Most protein-rich cereal: {most_protein}")

"""


@op
def extract():
    process = psutil.Process()
    start_cpu_usage = process.cpu_percent()
    start_mem_usage = process.memory_info().rss

    # res = requests.get("https://jsonplaceholder.typicode.com/users")
    # df = pd.read_csv('/usr/src/app/files/traffic.csv')
    with open('/usr/src/app/files/traffic.csv', 'r') as csvfile:
        reader = csv.reader(csvfile)
        rows = []
        for row in reader:
            rows.append(row)
        rows.pop(0)

    end_cpu_usage = process.cpu_percent()
    end_mem_usage = process.memory_info().rss
    cpu_usage_percent = end_cpu_usage - start_cpu_usage

    mem_usage_mb = (end_mem_usage - start_mem_usage) / 1024 / 1024
    with open('/usr/src/app/benchmark/log.txt', 'a+') as f:
        f.write(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB \n")
        logging.warning(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB")

    return rows


@op
def transform(data):
    process = psutil.Process()
    start_cpu_usage = process.cpu_percent()
    start_mem_usage = process.memory_info().rss

    transformed = []
    """for user in data:
        transformed.append({
            'ID': user['id'],
            'Name': user['name'],
            'Username': user['username'],
            'Email': user['email'],
            'Address': f"{user['address']['street']}, {user['address']['suite']}, {user['address']['city']}",
            'PhoneNumber': user['phone'],
            'Company': user['company']['name']
        })"""
    for line in data:
        transformed.append({
            'Date': line[0],
            'Category': line[1],
            'Browser': line[2],
            'Sessions': line[4],
            'Visitors': line[3],
        })
    end_cpu_usage = process.cpu_percent()
    end_mem_usage = process.memory_info().rss
    cpu_usage_percent = end_cpu_usage - start_cpu_usage

    mem_usage_mb = (end_mem_usage - start_mem_usage) / 1024 / 1024
    with open('/usr/src/app/benchmark/log.txt', 'a+') as f:
        f.write(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB \n")
        logging.warning(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB")

    return pd.DataFrame(transformed)


@op
def load(data):
    process = psutil.Process()
    start_cpu_usage = process.cpu_percent()
    start_mem_usage = process.memory_info().rss

    csv_path = "./files/transformed.csv"
    data.to_csv(path_or_buf=csv_path, index=False)

    end_cpu_usage = process.cpu_percent()
    end_mem_usage = process.memory_info().rss
    cpu_usage_percent = end_cpu_usage - start_cpu_usage

    mem_usage_mb = (end_mem_usage - start_mem_usage) / 1024 / 1024
    with open('/usr/src/app/benchmark/log.txt', 'a+') as f:
        f.write(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB \n")
        logging.warning(f"CPU Usage: {cpu_usage_percent}% ; Memory Usage: {mem_usage_mb} MB")
    return csv_path
