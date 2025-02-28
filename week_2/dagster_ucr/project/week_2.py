from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource


@op(
    required_resource_keys={"s3"},
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    description='List of Stocks'
)
def get_s3_data(context):
    output = list()
    s3_key = context.op_config['s3_key']
    s3_data = context.resources.s3.get_data(s3_key)
    for row in s3_data:
        stock = Stock.from_list(row)
        output.append(stock)
    return output


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"agg": Out(dagster_type=Aggregation)},
    description="Determine the Stock with the greatest high value",
)
def process_data(stocks):
    sorted_stocks = sorted(stocks, key=lambda x: x.high, reverse=True) 
    high_day = sorted_stocks[0].date
    high_high = sorted_stocks[0].high
    output = Aggregation(date=high_day, high=high_high)
    return output


@op(
    required_resource_keys={'redis'},
    ins={"agg": In(dagster_type=Aggregation)}
)
def put_redis_data(context, agg):
    context.resources.redis.put_data(str(agg.date), str(agg.high))


@graph
def week_2_pipeline():
    process = process_data(get_s3_data())
    redis_input = put_redis_data(process)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
