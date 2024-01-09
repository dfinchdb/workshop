# Databricks notebook source
import ast
import configparser
import pathlib

from pyspark.sql import SparkSession


def grant_permissions() -> None:
    """Grants a set of permissions on an object to a list of principals(users, groups, or service principals)
    If you would like to set different permissions for different principals or set of principals, then create
    a separate entry for each set of permissions. Remember that permissions are hierarchical. If you grant a
    principal permissions on a Catalog, then they will have at least that level of permissions on all objects
    in the Catalog.
    """
    # Read "grant_permissions_config.ini" & parse configs
    grant_permissions_config_path = "./grant_permissions_config.ini"
    grant_permissions_config = configparser.ConfigParser()
    grant_permissions_config.read(grant_permissions_config_path)
    permissions = ast.literal_eval(grant_permissions_config["options"]["permissions"])
    revoke_permissions = ast.literal_eval(grant_permissions_config["options"]["revoke"])

    # Get or Create SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Grant a set of permissions to a UC object for one or more principals(user, group, or service principal)
    for permission in permissions:
        object_name = permission["object_name"]
        object_type = permission["object_type"]
        grants = ", ".join(str(grant) for grant in permission["grants"])
        principals = permission["principals"]
        if revoke_permissions == True:
            for principal in principals:
                try:
                    spark.sql(
                        f"REVOKE {grants} ON {object_type} `{object_name}` FROM `{principal}`"
                    )
                    print(
                        f"Successful REVOKE {grants} ON {object_type} `{object_name}` FROM `{principal}`"
                    )
                except:
                    print(
                        f"Failed REVOKE {grants} ON {object_type} `{object_name}` FROM `{principal}`"
                    )
        else:
            for principal in principals:
                try:
                    spark.sql(
                        f"GRANT {grants} ON {object_type} `{object_name}` TO `{principal}`"
                    )
                    print(
                        f"Successful GRANT {grants} ON {object_type} `{object_name}` TO `{principal}`"
                    )
                except:
                    print(
                        f"Failed GRANT {grants} ON {object_type} `{object_name}` TO `{principal}`"
                    )


if __name__ == "__main__":
    grant_permissions()
