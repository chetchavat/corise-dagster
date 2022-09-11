from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
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


@asset(
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


@asset(
    required_resource_keys={'redis'},
    ins={"agg": In(dagster_type=Aggregation)}
)
def put_redis_data(context, agg):
    context.resources.redis.put_data(str(agg.date), str(agg.high))


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources()
