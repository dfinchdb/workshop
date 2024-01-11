# Databricks notebook source
# Import DLT and src/umpqua_poc_dlt
import dlt
import sys
import configparser
import ast

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

from umpqua_poc_dlt.umpqua_dlt import dlt_table

# COMMAND ----------


def generate_table(live_table):
    @dlt.table(
        name=live_table["name"],
        comment="Raw custom data capture for " + live_table["name"],
        table_properties=live_table["table_properties"],
    )
    def create_live_table():
        return dlt_table(spark, live_table.get("source"), live_table.get("options"))


# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

config = [
    {
        "name": "account_notification",
        "source": "abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/landing_zone/account_notification",
        "options": {
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
        },
        "table_properties": {
            "quality": "bronze",
            "pipelines.autoOptimize.managed": "true",
        },
    }
]

# COMMAND ----------

# dlt_config_path = "./umpqua_poc_dlt/umpqua_dlt_config.ini"
# dlt_config = configparser.ConfigParser()
# dlt_config.read(dlt_config_path)

# config = ast.literal_eval(dlt_config["dlt_config"]["config"])

# COMMAND ----------

[generate_table(table) for table in config]
