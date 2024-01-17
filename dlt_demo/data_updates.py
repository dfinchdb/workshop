from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

user_id = spark.sql('select current_user() as user').collect()[0]['user']
storage_account = "oneenvadls"
storage_container = "databricks-poc"

pii_source = f"file:/Workspace/Repos/{user_id}/workshop/fixtures/sample_data/updates/CustomerPIIData_update.csv"
# pii_source = "/Volumes/umpqua_poc_dev/bronze_data/bronze_volume/sample_data/csv_files/CustomerPIIData_update.csv"
pii_landing_destination = f"abfss://{storage_container}@{storage_account}.dfs.core.windows.net/umpqua_poc/landing_zone/customerpiidata"

pii_df = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("header", "True")
    .load(pii_source)
)

pii_df.write.csv(
    path=pii_landing_destination,
    sep="||",
    header=True,
    mode="append",
)
