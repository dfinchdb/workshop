from pyspark.sql import SparkSession

from project_config import (
    uc_object_config,
    uc_permissions_config,
)


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


def grant_permissions(spark, grants, object_type, object_name, principals):
    for principal in principals:
        try:
            spark.sql(
                f"GRANT {', '.join(str(grant) for grant in grants)} ON {object_type} `{object_name}` TO `{principal}`"
            )
            print(
                f"Successful GRANT {grants} ON {object_type} `{object_name}` TO `{principal}`"
            )
        except:
            print(
                f"Failed GRANT {grants} ON {object_type} `{object_name}` TO `{principal}`"
            )


def create_catalog(spark, catalog_name):
    spark.sql(
        f"""
        CREATE CATALOG IF NOT EXISTS {catalog_name}
        """
    )


def create_schema(spark, catalog_name, schema_name, location):
    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
            MANAGED LOCATION '{location}'
        """
    )


def create_volume(spark, catalog_name, schema_name, volume_name):
    spark.sql(
        f"""
        CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}
        """
    )


def create_external_location(
    spark,
    external_location_name,
    external_url,
    storage_credential_name,
    comment=None,
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
    for location in locations:
        create_external_location(
            spark,
            external_location_name=location.get("name"),
            external_url=location.get("location"),
            storage_credential_name=location.get("credential"),
            comment=location.get("comment"),
        )

    for catalog in catalogs:
        create_catalog(
            spark,
            catalog_name=catalog.get("catalog"),
        )

    for schema in schemas:
        create_schema(
            spark,
            catalog_name=schema.get("catalog"),
            schema_name=schema.get("schema"),
            location=schema.get("location"),
        )

    for volume in volumes:
        create_volume(
            spark,
            catalog_name=volume.get("catalog"),
            schema_name=volume.get("schema"),
            volume_name=volume.get("volume"),
        )

    if tables is not None:
        for table in tables:
            create_sample_table(
                spark,
                catalog_name=table.get("catalog"),
                schema_name=table.get("schema"),
                table_name=table.get("table"),
            )


if __name__ == "__main__":
    spark = get_sparksession()
    dbutils = get_dbutils(spark)

    project_name = "umpqua_poc"
    storage_account_name = "oneenvadls"
    storage_container_name = "umpquapocdev"
    storage_credential_name = (
        "121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101"
    )

    project_config = uc_object_config(
        project_name,
        storage_account_name,
        storage_container_name,
        storage_credential_name,
    )

    permissions = uc_permissions_config(project_name)

    create_uc_objects(
        spark,
        project_config.get("catalogs"),
        project_config.get("schemas"),
        project_config.get("volumes"),
        project_config.get("locations"),
    )

    for permission in permissions:
        grant_permissions(
            spark,
            grants=permission.get("grants"),
            object_type=permission.get("object_type"),
            object_name=permission.get("object_name"),
            principals=permission.get("principal"),
        )
