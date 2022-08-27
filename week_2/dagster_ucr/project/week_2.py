from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource
from week_2.dagster_ucr import resources


@op(
    required_resource_keys={"s3_resource"},
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List(Stock))},
    description='List of Stocks'
)
def get_s3_data(context):
    output = List()
    output.append(context.resources.get_data())
    return output



@op(
    config_schema={'nlargest': int},
    ins={"stocks": In(dagster_type=List)},
    out=DynamicOut(),
    description="Determine the Stock with the greatest high value",
)
def process_data(context, stocks):
    nlargest = context.op_config['nlargest']
    if nlargest is None:
        nlargest = 1
    sorted_stocks = sorted(stocks, key=lambda x: x.high, reverse=True) 
    for n in range(0, nlargest):
        high_day = sorted_stocks[n].date
        high_high = sorted_stocks[n].high
        output = Aggregation(date=high_day, high=high_high)
        yield DynamicOutput(output, mapping_key=str(n))


@op(
    required_resource_keys={'redis_resource'},
    ins={"agg": In(dagster_type=Aggregation)}
)
def put_redis_data(context, agg):
    context.resources.put_data()


@graph
def week_2_pipeline():
    process = process_data(get_s3_data())
    redis_input = process.map(put_redis_data)


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
                "endpoint_url": "http://host.docker.internal:4566",
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
