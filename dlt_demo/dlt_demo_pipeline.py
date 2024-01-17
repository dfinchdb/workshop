# Databricks notebook source
# Import DLT and src/umpqua_poc_dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

storage_account = "oneenvadls"
storage_container = "databricks-poc"

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
            f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/umpqua_poc/landing_zone/customerpiidata"
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
            f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/umpqua_poc/landing_zone/customergtlimits"
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
    pii_df = dlt.read_stream("customerpiidata_clean").filter(col("is_company") == 1)
    limit_df = dlt.read_stream("customergtlimits")
    columns = [
        "customergtlimits.customer_id",
        "customergtlimits.customer_name",
        "customergtlimits.token_required_limit",
        "customergtlimits.dual_approval_limit",
        "customergtlimits.limit_per_transaction",
        "customergtlimits.limit_per_day",
        "customergtlimits.limit_per_acct_per_day",
        "customergtlimits.limit_per_month",
        "customergtlimits.aggregate_ach_limit_per_day",
        "customerpiidata_clean.group_id",
        "customerpiidata_clean.group_name",
        "customerpiidata_clean.tax_id",
        "customerpiidata_clean.is_company",
        "customerpiidata_clean.is_treasury",
        "customerpiidata_clean.primary_cif",
        "customerpiidata_clean.service_charge_plan_id",
        "customerpiidata_clean.plan_name",
        "customerpiidata_clean.charge_account",
        "customerpiidata_clean.create_date",
        "customerpiidata_clean.street_address1",
        "customerpiidata_clean.street_address2",
        "customerpiidata_clean.city",
        "customerpiidata_clean.state",
        "customerpiidata_clean.postal_code",
        "customerpiidata_clean.province",
        "customerpiidata_clean.is_international",
        "customerpiidata_clean.iso_code_a3",
    ]
    return pii_df.join(
        limit_df, pii_df.customer_id == limit_df.customer_id, "left"
    ).select(columns)


# COMMAND ----------
