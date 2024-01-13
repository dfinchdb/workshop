import ast
import configparser
import pathlib
import sys

from pyspark.sql import SparkSession


def get_sparksession():
    return SparkSession.builder.getOrCreate()


def get_dbutils(spark: SparkSession):
    dbutils = None

    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)

    else:
        import IPython

        dbutils = IPython.get_ipython().user_ns["dbutils"]

    return dbutils


def create_catalog(spark, catalog_name):
    spark.sql(
        f"""
        CREATE CATALOG IF NOT EXISTS {catalog_name}
        """
    )


def create_schema(spark, catalog_name, schema_name):
    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}"""
    )


def create_volume(spark, catalog_name, schema_name, volume_name):
    spark.sql(
        f"""
        CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}
        """
    )


def create_external_location(
    spark, external_location_name, external_url, storage_credential_name, comment=None
):
    spark.sql(
        f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `{external_location_name}` URL '{external_url}'
            WITH (CREDENTIAL `{storage_credential_name}`)
            COMMENT '{comment}'
        """
    )


def create_sample_table(spark, catalog_name, schema_name, table_name):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.{table_name} (id INT, name STRING)
        """
    )
    spark.sql(
        f"""
        INSERT INTO {catalog_name}.{schema_name}.{table_name} VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
        """
    )


if __name__ == "__main__":
    spark = get_sparksession()
    dbutils = get_dbutils(spark)

    catalogs = [{"catalog": "df_test_catalog"}]
    schemas = [{"catalog": "df_test_catalog", "schema": "df_test_schema"}]
    volumes = [
        {
            "catalog": "df_test_catalog",
            "schema": "df_test_schema",
            "volume": "test_volume",
        }
    ]
    tables = [
        {
            "catalog": "df_test_catalog",
            "schema": "df_test_schema",
            "table": "sample_table",
        }
    ]
    locations = [
        {
            "name": "df_test_location",
            "location": "abfss://umpquapocprd@oneenvadls.dfs.core.windows.net/umpqua_poc/playground",
            "credential": "121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101",
            "comment": "",
        }
    ]

    for catalog in catalogs:
        create_catalog(spark, catalog.get("catalog"))

    for schema in schemas:
        create_schema(spark, schema.get("catalog"), schema.get("schema"))

    for volume in volumes:
        create_volume(
            spark, volume.get("catalog"), volume.get("schema"), volume.get("volume")
        )

    for table in tables:
        create_sample_table(
            spark, table.get("catalog"), table.get("schema"), table.get("table")
        )

    for location in locations:
        create_external_location(
            spark,
            location.get("name"),
            location.get("location"),
            location.get("credential"),
            location.get("comment"),
        )
