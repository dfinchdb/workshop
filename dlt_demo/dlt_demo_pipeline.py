# Databricks notebook source
# Import DLT and src/umpqua_poc_dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------


@dlt.table(
    name="customerpiidata",
    comment="Raw custom data capture for customerpiidata",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def customerpiidata_dlt():
    return (
        spark.readStream.format("cloudFiles")
        .options(
            **{
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
        .load(
            "abfss://databricks-poc@oneenvadls.dfs.core.windows.net/umpqua_poc/landing_zone/customerpiidata"
        )
    )


@dlt.table(
    name="customergtlimits",
    comment="Raw custom data capture for customergtlimits",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
)
def customergtlimits_dlt():
    return (
        spark.readStream.format("cloudFiles")
        .options(
            **{
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
        .load(
            "abfss://databricks-poc@oneenvadls.dfs.core.windows.net/umpqua_poc/landing_zone/customergtlimits"
        )
    )


@dlt.table(
    name="customerpiidata_clean",
    comment="Cleaned customerpiidata",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_or_drop("valid_tax_id", "tax_id IS NOT NULL")
def customerpiidata_clean_dlt():
    return dlt.read_stream("customerpiidata")


@dlt.table(
    name="corporate_customer_data",
    comment="Clean & combined data about corporate customers only",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def corporate_customer_data_dlt():
    return (
        dlt.read_stream("customerpiidata_clean")
        .filter(col("is_company") == 1)
        .join(
            dlt.read_stream("customergtlimits"),
            ["customer_id", "customer_name"],
            "left",
        )
    )


# COMMAND ----------
