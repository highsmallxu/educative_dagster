from dagster import (
    Definitions,
    define_asset_job,
)
from dagster_gcp_pandas import bigquery_pandas_io_manager
from dagster_gcp import bigquery_resource
from dagster import (
    asset,
    MetadataValue,
    schedule,
    ScheduleEvaluationContext,
    RunRequest,
    AssetSelection,
)
import requests
import pandas as pd
import logging
import json
import os

# setup GCP credentials 
gcp_auth_key = <<gcp_auth>>
with open('auth.json', 'w') as outfile:
    json.dump(gcp_auth_key, outfile)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "auth.json"
PROJECT_ID = json.load(open("auth.json", "rb"))["quota_project_id"]

FROM_CUR = "EUR"
TO_CUR = "USD"

@asset(io_manager_key="bq_io_manager", group_name="exchange_rate_fact")
def exchange_rate_staging(context):
    # use the date of the scheduled date which is fixed for each run
    date = context.get_tag("date")
    frankfurter_api = f"https://api.frankfurter.app/{date}?from={FROM_CUR}&to={TO_CUR}"
    response = requests.get(frankfurter_api).json()
    context.add_output_metadata(
        {
            "preview": MetadataValue.json(response),
        }
    )
    return pd.DataFrame(
        {
            "timestamp": [pd.Timestamp.now().round("min")],
            "date": [pd.Timestamp(date)],
            "from_cur": [FROM_CUR],
            "to_cur": [TO_CUR],
            "rate": [response["rates"][TO_CUR]],
        }
    )


@asset(
    io_manager_def=None,
    required_resource_keys={"bigquery"},
    group_name="exchange_rate_fact",
)
def exchange_rate(context, exchange_rate_staging):
    with open("my_dagster_project/sql/exchange_rate.sql") as f:
        query = f.read()
    return context.resources.bigquery.query(
        (query),
    ).result()


@asset(
    io_manager_def=None,
    required_resource_keys={"bigquery"},
    group_name="exchange_rate_agg",
)
def exchange_rate_report(context, exchange_rate):
    logging.error("report")
    with open("my_dagster_project/sql/exchange_rate_report.sql") as f:
        query = f.read()
    return context.resources.bigquery.query(
        (query),
    ).result()


@schedule(
    job=define_asset_job(
        name="exchange_rate_job", selection=AssetSelection.groups("exchange_rate_fact")
    ),
    cron_schedule="* * * * *",
)
def exchange_rate_schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(tags={"date": scheduled_date})


@schedule(
    job=define_asset_job(
        name="exchange_rate_report_job",
        selection=AssetSelection.groups("exchange_rate_agg"),
    ),
    cron_schedule="*/5 * * * *",
)
def exchange_rate_report_schedule(context: ScheduleEvaluationContext):
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return RunRequest(tags={"date": scheduled_date})


defs = Definitions(
    assets=[exchange_rate_staging, exchange_rate, exchange_rate_report],
    schedules=[exchange_rate_schedule, exchange_rate_report_schedule],
    resources={
        "bq_io_manager": bigquery_pandas_io_manager.configured(
            {"project": PROJECT_ID, "dataset": "dagster"}
        ),
        "bigquery": bigquery_resource.configured(
            {
                "project": PROJECT_ID,
            }
        ),
    },
)
