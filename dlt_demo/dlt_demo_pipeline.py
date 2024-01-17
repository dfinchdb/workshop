# Databricks notebook source
# Import DLT and src/umpqua_poc_dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

from dlt_demo_setup.dlt_demo import (
    raw_tables,
    customerpiidata_clean,
    corporate_customer_data,
)

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
    return raw_tables(
        spark,
        source_path="abfss://databricks-poc@oneenvadls.dfs.core.windows.net/umpqua_poc/landing_zone/customerpiidata"
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
    return raw_tables(
        spark,
        source_path="abfss://databricks-poc@oneenvadls.dfs.core.windows.net/umpqua_poc/landing_zone/customergtlimits"
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
    return customerpiidata_clean()


@dlt.table(
    name="corporate_customer_data",
    comment="Clean & combined data about corporate customers only",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
def corporate_customer_data_dlt():
    return corporate_customer_data()


# COMMAND ----------
