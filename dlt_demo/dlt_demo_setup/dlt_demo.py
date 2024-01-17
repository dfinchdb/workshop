"""
Docs:
AutoLoader Options - https://docs.databricks.com/en/ingestion/auto-loader/options.html
CSV Options - https://docs.databricks.com/en/ingestion/auto-loader/options.html#csv-options
Data Quality - https://docs.databricks.com/en/delta-live-tables/expectations.html
Event Hooks - https://docs.databricks.com/en/delta-live-tables/event-hooks.html
Monitoring - https://docs.databricks.com/en/delta-live-tables/observability.html
"""

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def raw_tables(source_path: str) -> DataFrame:
    df = (
        dlt.readStream.format("cloudFiles")
        .options(
            {
                "cloudFiles.format": "csv",
                "header": "true",
                "delimiter": "||",
                "rescuedDataColumn": "_rescued_data",
                "cloudFiles.validateOptions": "true",
                "cloudFiles.useNotifications": "false",
                "cloudFiles.inferColumnTypes": "true",
                "cloudFiles.backfillInterval": "1 day",
                "cloudFiles.schemaEvolutionMode": "rescue",
                "cloudFiles.allowOverwrites": "false",
            }
        )
        .load(source_path)
    )
    return df


def customerpiidata_clean() -> DataFrame:
    df = dlt.read_stream("customerpiidata")
    return df


def corporate_customer_data() -> DataFrame:
    pii_df = dlt.read_stream("customerpiidata_clean").filter(col("is_company") == 1)
    limits_df = dlt.read_stream("customergtlimits")
    df = pii_df.join(limits_df, pii_df.customer_id == limits_df.customer_id, "left")
    return df


if __name__ == "__main__":
    pass
