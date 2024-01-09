import ast
import configparser
import pathlib

from pyspark.sql import SparkSession


def get_dbutils(spark: SparkSession):
    dbutils = None

    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)

    else:
        import IPython

        dbutils = IPython.get_ipython().user_ns["dbutils"]

    return dbutils


def cleanup_files(dbutils, dest_dir: str) -> None:
    """Removes the copied files & directory from UC Volumes

    Args:
        dest_dir (str): Destination location.
            For UC Volumes it takes the form /Volumes/<my_catalog>/<my_schema>/<my_volume>/<path>/<to>/<directory>
            For further information see https://docs.databricks.com/en/files/index.html
    """
    dbutils.fs.rm(dest_dir, recurse=True)


def upload_dir() -> None:
    """Copy a directory using dbutils.
    For this use case it is used as a convenient way to copy files located in the fixtures directory of this repo to UC Volumes.
    When using the Databricks extension for vsCode, files are synchronized to the user's Workspace & can then be copied to Volumes.
    """
    # Read "upload_files_config.ini" & parse configs
    upload_files_config_path = pathlib.Path(__file__).parent / "upload_files_config.ini"
    upload_files_config = configparser.ConfigParser()
    upload_files_config.read(upload_files_config_path)
    file_cleanup = ast.literal_eval(upload_files_config["options"]["file_cleanup"])
    paths = ast.literal_eval(upload_files_config["paths"]["uc_paths"])

    # Get or Create SparkSession & dbutils
    spark = SparkSession.builder.getOrCreate()
    dbutils = get_dbutils(spark)

    # Removes copied files IF "file_cleanup" is set to True in "upload_files_config.ini"
    if file_cleanup == True:
        for path in paths:
            cleanup_files(dbutils, path["destination"])
    else:
        for path in paths:
            db_source = pathlib.Path(__file__).parents[2]
            source_dir = f'file:{str(db_source)}{path["source"]}'
            dest_dir = path["destination"]
            dbutils.fs.cp(source_dir, dest_dir, recurse=True)


if __name__ == "__main__":
    upload_dir()
