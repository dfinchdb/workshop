# Databricks notebook source
# MAGIC %md
# MAGIC #### Simulate new data being produced with a change in the schema
# MAGIC - Reads "||" delim file from bronze_volume/sample_data/updates folder

# COMMAND ----------

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", "||")
    .load(
        "/Volumes/umpqua_poc_dev/bronze_data/bronze_volume/sample_data/updates/pipeline_update.csv"
    )
)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Simulate New File Arrival
# MAGIC - The data from previous command is written to the "Account Notification" location
# MAGIC - DLT w/ AutoLoader will detect the file the next time the pipeline is triggered, & will address the schema change based on the mode chosen
# MAGIC   - addNewColumn: In this mode the schema change will automatically be integrated into the table.
# MAGIC   - rescue: In this mode the row with the new column will be processed, but the new column will be moved to the _rescued_data_column for further processing

# COMMAND ----------

destination = "abfss://umpquapocdev@oneenvadls.dfs.core.windows.net/umpqua_poc/landing_zone/account_notification"

df.write.csv(
    path=destination,
    sep="||",
    header=True,
    mode="append",
)
