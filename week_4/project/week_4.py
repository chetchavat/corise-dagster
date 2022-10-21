from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    description="List of Stocks",
    group_name="corise"
)
def get_s3_data(context):
    output = list()
    s3_key = context.op_config['s3_key']
    s3_data = context.resources.s3.get_data(s3_key)
    for row in s3_data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@asset(
    description="Determine the Stock with the greatest high value",
    group_name="corise"
)
def process_data(get_s3_data):
    sorted_stocks = sorted(get_s3_data, key=lambda x: x.high, reverse=True) 
    high_day = sorted_stocks[0].date
    high_high = sorted_stocks[0].high
    output = Aggregation(date=high_day, high=high_high)
    return output


@asset(
    required_resource_keys={'redis'},
    group_name="corise"
)
def put_redis_data(context, process_data):
    context.resources.redis.put_data(str(process_data.date), str(process_data.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    },
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
)
