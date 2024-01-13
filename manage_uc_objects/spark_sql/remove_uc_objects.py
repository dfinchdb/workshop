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
                    DROP EXTERNAL LOCATION IF EXISTS {external_location_name}
                    """
                )
                print(f"Dropped External Location: {external_location_name}")
            except:
                print(f"Unable to drop External Location: {external_location_name}")


def drop_uc_objects(spark, dbutils, catalogs, external_locations):
    drop_catalogs(spark, dbutils, catalogs)
    drop_external_locations(spark, external_locations)


if __name__ == "__main__":
    spark = get_sparksession()
    dbutils = get_dbutils(spark)

    catalogs = ["df_test_catalog"]
    external_locations = ["df_test_location"]

    drop_uc_objects(spark, dbutils, catalogs, external_locations)
