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
        CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
        """
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


def create_uc_objects(spark, catalogs, schemas, volumes, locations, tables=None):
    for catalog in catalogs:
        create_catalog(spark, catalog.get("catalog"))

    for schema in schemas:
        create_schema(spark, schema.get("catalog"), schema.get("schema"))

    for volume in volumes:
        create_volume(
            spark, volume.get("catalog"), volume.get("schema"), volume.get("volume")
        )

    for location in locations:
        create_external_location(
            spark,
            location.get("name"),
            location.get("location"),
            location.get("credential"),
            location.get("comment"),
        )

    for table in tables:
        create_sample_table(
            spark, table.get("catalog"), table.get("schema"), table.get("table")
        )


def project_uc_object_config(
    project_name, storage_account_name, storage_credential_name
):
    project_config = dict()
    project_config["catalogs"] = [{"catalog": f"{project_name}_dev"}]
    project_config["schemas"] = [
        {"catalog": f"{project_name}_dev", "schema": "bronze_data"},
        {"catalog": f"{project_name}_dev", "schema": "silver_data"},
        {"catalog": f"{project_name}_dev", "schema": "gold_data"},
    ]
    project_config["volumes"] = [
        {
            "catalog": f"{project_name}_dev",
            "schema": "bronze_data",
            "volume": "bronze_volume",
        },
        {
            "catalog": f"{project_name}_dev",
            "schema": "silver_data",
            "volume": "silver_volume",
        },
        {
            "catalog": f"{project_name}_dev",
            "schema": "gold_data",
            "volume": "gold_volume",
        },
    ]

    project_config["locations"] = [
        {
            "name": f"{project_name}_dev_bronze_schema_ext_loc",
            "location": f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/tables/bronze",
            "credential": f"{storage_credential_name}",
            "comment": f"External location for the bronze schema of the dev catalog for project {project_name}",
        },
        {
            "name": f"{project_name}_dev_silver_schema_ext_loc",
            "location": f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/tables/silver",
            "credential": "121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101",
            "comment": f"External location for the silver schema of the dev catalog for project {project_name}",
        },
        {
            "name": f"{project_name}_dev_gold_schema_ext_loc",
            "location": f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/tables/gold",
            "credential": f"{storage_credential_name}",
            "comment": f"External location for the gold schema of the dev catalog for project {project_name}",
        },
        {
            "name": f"{project_name}_dev_bronze_volume_ext_loc",
            "location": f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/volumes/bronze",
            "credential": f"{storage_credential_name}",
            "comment": f"External location for the bronze volume of the dev catalog for project {project_name}",
        },
        {
            "name": f"{project_name}_dev_silver_volume_ext_loc",
            "location": f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/volumes/silver",
            "credential": f"{storage_credential_name}",
            "comment": f"External location for the silver volume of the dev catalog for project {project_name}",
        },
        {
            "name": f"{project_name}_dev_gold_volume_ext_loc",
            "location": f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/volumes/gold",
            "credential": f"{storage_credential_name}",
            "comment": f"External location for the gold volume of the dev catalog for project {project_name}",
        },
        {
            "name": f"{project_name}_dev_landing_zone_ext_loc",
            "location": f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/landing_zone",
            "credential": f"{storage_credential_name}",
            "comment": f"External location for the landing zone of the dev catalog for project {project_name}",
        },
        {
            "name": f"{project_name}_dev_playground_ext_loc",
            "location": f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/playground",
            "credential": f"{storage_credential_name}",
            "comment": f"External location for the playground of the dev catalog for project {project_name}",
        },
    ]
    return project_config


if __name__ == "__main__":
    spark = get_sparksession()
    dbutils = get_dbutils(spark)

    project_name = "umpqua_poc"
    storage_account_name = "oneenvadls"
    storage_container_name = "umpquapocdev"
    storage_credential_name = (
        "121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101"
    )
    project_config = project_uc_object_config(
        project_name, storage_account_name, storage_credential_name
    )

    create_uc_objects(
        spark,
        project_config.get("catalogs"),
        project_config.get("schemas"),
        project_config.get("volumes"),
        project_config.get("locations"),
    )
