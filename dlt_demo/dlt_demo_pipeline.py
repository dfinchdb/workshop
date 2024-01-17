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
        "limit_df.customer_id",
        "limit_df.customer_name",
        "limit_df.token_required_limit",
        "limit_df.dual_approval_limit",
        "limit_df.limit_per_transaction",
        "limit_df.limit_per_day",
        "limit_df.limit_per_acct_per_day",
        "limit_df.limit_per_month",
        "limit_df.aggregate_ach_limit_per_day",
        "pii_df.group_id",
        "pii_df.group_name",
        "pii_df.tax_id",
        "pii_df.is_company",
        "pii_df.is_treasury",
        "pii_df.primary_cif",
        "pii_df.service_charge_plan_id",
        "pii_df.plan_name",
        "pii_df.charge_account",
        "pii_df.create_date",
        "pii_df.street_address1",
        "pii_df.street_address2",
        "pii_df.city",
        "pii_df.state",
        "pii_df.postal_code",
        "pii_df.province",
        "pii_df.is_international",
        "pii_df.iso_code_a3",
    ]
    return pii_df.join(
        limit_df, pii_df.customer_id == limit_df.customer_id, "left"
    ).select(columns)


[
    "customer_id",
    "customer_name",
    "token_required_limit",
    "dual_approval_limit",
    "limit_per_transaction",
    "limit_per_day",
    "limit_per_acct_per_day",
    "limit_per_month",
    "aggregate_ach_limit_per_day",
    "group_id",
    "group_name",
    "tax_id",
    "is_company",
    "is_treasury",
    "primary_cif",
    "service_charge_plan_id",
    "plan_name",
    "charge_account",
    "create_date",
    "street_address1",
    "street_address2",
    "city",
    "state",
    "postal_code",
    "province",
    "is_international",
    "iso_code_a3",
]

# COMMAND ----------
