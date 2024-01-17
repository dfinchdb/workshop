user_id = spark.sql('select current_user() as user').collect()[0]['user']
storage_account = "oneenvadls"
storage_container = "databricks-poc"

pii_source = f"file:/Workspace/Repos/{user_id}/workshop/fixtures/sample_data/updates/CustomerPIIData_update.csv"
pii_volume_destination = f"/Volumes/umpqua_poc_dev/bronze_data/bronze_volume/sample_data/pipe_delim_files/customerpiidata"
pii_landing_destination = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/umpqua_poc/landing_zone/customerpiidata"
pii_table_destination = "umpqua_poc.bronze_data.test_customerpiidata"
limits_source = f"file:/Workspace/Repos/{user_id}/workshop/fixtures/sample_data/updates/CustomerGTLimits_update.csv"
limits_volume_destination = f"/Volumes/umpqua_poc_dev/bronze_data/bronze_volume/sample_data/pipe_delim_files/customergtlimits"
limits_landing_destination = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/umpqua_poc/landing_zone/customergtlimits"
limits_table_destination = "umpqua_poc.bronze_data.test_customergtlimits"

pii_df = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "True")
    .load(pii_source)
)

pii_df.write.csv(
    path=pii_volume_destination,
    sep="||",
    header=True,
    mode="overwrite",
)

pii_df.write.csv(
    path=pii_landing_destination,
    sep="||",
    header=True,
    mode="overwrite",
)

pii_df.write.format("delta").mode("overwrite").saveAsTable(pii_table_destination)

limits_df = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "True")
    .load(limits_source)
)

limits_df.write.csv(
    path=pii_volume_destination,
    sep="||",
    header=True,
    mode="overwrite",
)

limits_df.write.csv(
    path=pii_landing_destination,
    sep="||",
    header=True,
    mode="overwrite",
)

limits_df.write.format("delta").mode("overwrite").saveAsTable(limits_table_destination)
