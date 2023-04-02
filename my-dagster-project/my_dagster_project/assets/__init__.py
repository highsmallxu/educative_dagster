from github import InputFileContent
import nbformat
import jupytext
import pickle
from nbconvert.preprocessors import ExecutePreprocessor
import pandas as pd
from datetime import timedelta
from dagster import asset, MetadataValue, DailyPartitionsDefinition, op, job, schedule, ScheduleEvaluationContext, RunRequest
from github import Github
import requests
import logging

FROM_CUR = "EUR"
TO_CUR = "USD"


@op(config_schema={"scheduled_date": str})
def exchange_rate_from_frankfurter(context):
    context.log.info(context.op_config["scheduled_date"])


@job
def exchange_rate_job():
    exchange_rate_from_frankfurter()

# @asset(config_schema={"scheduled_date": str})
# def exchange_rate_from_frankfurter(context):
#     # partition_date_str = context.asset_partition_key_for_output()
#     # return pd.read_csv(f"coolweatherwebsite.com/weather_obs&date={partition_date_str}")
#     # context.log.info(context.resources)
#     # context.log.info(context.op_config)
#     # context.log.info(context.asset_partition_key_for_output())
#     context.log.info(context.op_config["scheduled_date"])
#     context.log.info(context.get_tag("date"))
#     # frankfurter_api = f"https://api.frankfurter.app/{key}?from={FROM_CUR}&to={TO_CUR}"
#     # response = requests.get(frankfurter_api).json()
#     # # response = {"from_cur":"EUR", "to_cur":"USD", "timestamp":"xxx", "rate":response["rates"]["USD"]}
#     # context.add_output_metadata(
#     #     {
#     #         "preview": MetadataValue.json(response),
#     #     }
#     # )
#     # return pd.DataFrame.from_dict(response)

# # @asset(partitions_def=DailyPartitionsDefinition(start_date="2023-03-24"))
# # def my_daily_partitioned_asset(context) -> pd.DataFrame:
# #     partition_date_str = context.asset_partition_key_for_output()
# #     return pd.read_csv(f"coolweatherwebsite.com/weather_obs&date={partition_date_str}")

