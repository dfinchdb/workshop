import ast
import configparser
import pathlib
import sys

from pyspark.sql import SparkSession

from project_config import uc_object_config


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


def drop_tables(spark, catalog_name, schema_name, table_names):
    if len(table_names) > 0:
        print(f"Found Tables In {catalog_name}.{schema_name}: {table_names}")

        for table_name in table_names:
            try:
                spark.sql(
                    f"""
                    DROP TABLE {catalog_name}.{schema_name}.{table_name}
                    """
                )
                print(f"Dropped Table: {catalog_name}.{schema_name}.{table_name}")
            except:
                print(
                    f"Unable to drop Table: {catalog_name}.{schema_name}.{table_name}"
                )


def drop_volumes(spark, dbutils, catalog_name, schema_name, volume_names):
    if len(volume_names) > 0:
        print(f"Found Volumes In {catalog_name}.{schema_name}: {volume_names}")

        for volume_name in volume_names:
            try:
                dbutils.fs.rm(
                    f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/",
                    recurse=True,
                )
                print(
                    f"Deleted the root directory of Volume: {catalog_name}.{schema_name}.{volume_name}"
                )
            except:
                print(
                    f"Unable to delete Volume root: /Volumes/{catalog_name}/{schema_name}/{volume_name}/"
                )

            try:
                spark.sql(
                    f"""
                    DROP VOLUME {catalog_name}.{schema_name}.{volume_name}
                    """
                )
                print(f"Dropped Volume: {catalog_name}.{schema_name}.{volume_name}")
            except:
                print(
                    f"Unable to drop Volume: {catalog_name}.{schema_name}.{volume_name}"
                )


def drop_schemas(spark, dbutils, catalog_name, schema_names):
    schema_names = [i for i in schema_names if i != "information_schema"]
    if len(schema_names) > 0:
        print(f"Found Schemas In Catalog {catalog_name}: {schema_names}")

        for schema_name in schema_names:
            try:
                print(f"Checking for Tables in Schema: {catalog_name}.{schema_name}")
                table_names = [
                    x.tableName
                    for x in spark.sql(
                        f"""
                        SHOW TABLES IN {catalog_name}.{schema_name}
                        """
                    ).collect()
                ]
            except:
                print(f"Failed to list Tables in Schema: {catalog_name}.{schema_name}")
            drop_tables(spark, catalog_name, schema_name, table_names)

            try:
                print(f"Checking for Volumes in Schema: {catalog_name}.{schema_name}")
                volume_names = [
                    x.volume_name
                    for x in spark.sql(
                        f"""
                        SHOW VOLUMES IN {catalog_name}.{schema_name}
                        """
                    ).collect()
                ]
            except:
                print(f"Failed to list Volumes in Schema: {catalog_name}.{schema_name}")
            drop_volumes(spark, dbutils, catalog_name, schema_name, volume_names)

            try:
                spark.sql(
                    f"""
                    DROP SCHEMA {catalog_name}.{schema_name}
                    """
                )
                print(f"Dropped Schema: {catalog_name}.{schema_name}")
            except:
                print(f"Unable to drop Schema: {catalog_name}.{schema_name}")


def drop_catalog(spark, dbutils, catalog_name):
    try:
        print(f"Checking for Schemas in Catalog: {catalog_name}")
        schema_names = [
            x.databaseName
            for x in spark.sql(
                f"""
                SHOW SCHEMAS IN {catalog_name}
                """
            ).collect()
        ]
    except:
        schema_names = []
        print(f"Failed to list Schemas in Catalog: {catalog_name}")

    drop_schemas(spark, dbutils, catalog_name, schema_names)

    try:
        spark.sql(
            f"""
            DROP CATALOG {catalog_name}
            """
        )
        print(f"Dropped Catalog: {catalog_name}")
    except:
        print(f"Unable to drop Catalog: {catalog_name}")


def drop_catalogs(spark, dbutils, catalog_names):
    if len(catalog_names) > 0:
        for catalog_name in catalog_names:
            print(f"Attempting to drop Catalog: {catalog_name}")
            drop_catalog(spark, dbutils, catalog_name)


def drop_external_locations(spark, external_location_names):
    if len(external_location_names) > 0:
        for external_location_name in external_location_names:
            print(f"Attempting to drop External Location: {external_location_name}")
            try:
                spark.sql(
                    f"""
                    DROP EXTERNAL LOCATION IF EXISTS {external_location_name} FORCE
                    """
                )
                print(f"Dropped External Location: {external_location_name}")
            except:
                print(f"Unable to drop External Location: {external_location_name}")


def delete_container_root(dbutils, storage_account_name, storage_container_name):
    dbutils.fs.rm(
        f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}',
        recurse=True,
    )


def drop_uc_objects(spark, dbutils, catalogs, external_locations):
    drop_catalogs(spark, dbutils, catalogs)
    drop_external_locations(spark, external_locations)


if __name__ == "__main__":
    spark = get_sparksession()
    dbutils = get_dbutils(spark)

    project_name = "umpqua_poc"
    storage_account_name = "oneenvadls"
    storage_container_name = "databricks-poc"
    storage_credential_name = "ubdatabrickspocdevtestsc"

    project_config = uc_object_config(
        project_name,
        storage_account_name,
        storage_container_name,
        storage_credential_name,
    )

    catalogs = [x.get("catalog") for x in project_config.get("catalogs")]
    external_locations = [x.get("name") for x in project_config.get("locations")]

    # catalogs = ["umpqua_poc_dev", "umpqua_poc_prd"]
    # external_locations = [
    #     "umpqua_poc_dev_bronze_schema_ext_loc",
    #     "umpqua_poc_dev_silver_schema_ext_loc",
    #     "umpqua_poc_dev_gold_schema_ext_loc",
    #     "umpqua_poc_dev_bronze_volume_ext_loc",
    #     "umpqua_poc_dev_silver_volume_ext_loc",
    #     "umpqua_poc_dev_gold_volume_ext_loc",
    #     "umpqua_poc_dev_landing_zone_ext_loc",
    #     "umpqua_poc_dev_playground_ext_loc",
    #     "umpqua_poc_prd_bronze_schema_ext_loc",
    #     "umpqua_poc_prd_silver_schema_ext_loc",
    #     "umpqua_poc_prd_gold_schema_ext_loc",
    #     "umpqua_poc_prd_bronze_volume_ext_loc",
    #     "umpqua_poc_prd_silver_volume_ext_loc",
    #     "umpqua_poc_prd_gold_volume_ext_loc",
    #     "umpqua_poc_prd_landing_zone_ext_loc",
    #     "umpqua_poc_prd_playground_ext_loc",
    # ]

    drop_uc_objects(spark, dbutils, catalogs, external_locations)
