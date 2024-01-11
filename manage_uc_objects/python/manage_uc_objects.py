import ast
import configparser
import pathlib
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
from databricks.sdk.service.catalog import AzureManagedIdentity


def create_catalog(client: WorkspaceClient, catalog_name: str) -> catalog.CatalogInfo:
    """Creates a Catalog in Unity Catalog & returns catalogInfo object

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        catalog_name (str): Name of the Catalog to create

    Returns:
        catalog.CatalogInfo: Object containing information about UC Catalog
    """
    existing_catalog = get_catalog(client, catalog_name)
    if existing_catalog is not None:
        print(f"Catalog already exists: {catalog_name}")
        return existing_catalog
    else:
        try:
            created_catalog = client.catalogs.create(name=catalog_name)
            print(f"Created catalog: {catalog_name}")
        except:
            print(f"Unable to create catalog: {catalog_name}")
            sys.exit(1)
        return created_catalog


def get_catalog(client: WorkspaceClient, catalog_name: str) -> catalog.CatalogInfo:
    """Checks if Catalog Exists & returns catalogInfo object if found

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        catalog_name (str): Name of the Catalog to retrieve

    Returns:
        catalog.CatalogInfo: Object containing information about UC Catalog
    """
    all = client.catalogs.list()
    try:
        existing_catalog = [x for x in all if x.name == catalog_name][0]
    except:
        existing_catalog = None
        print(f"Unable to locate catalog: {catalog_name}")
    return existing_catalog


def delete_catalog(client: WorkspaceClient, catalog_name: str) -> None:
    """Checks if the Catalog exists & deletes it if found

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        catalog_name (str): Name of the Catalog to delete
    """
    existing_catalog = get_catalog(client, catalog_name)
    if existing_catalog is None:
        print(f"Attempting to delete catalog which does not exist: {catalog_name}")
    else:
        try:
            client.catalogs.delete(name=existing_catalog.name, force=True)
            print(f"Deleted catalog: {catalog_name}")
        except:
            print(f"Unable to delete catalog: {catalog_name}")


def create_schema(client: WorkspaceClient, schema_name: str):
    """Creates Schema in Unity Catalog & returns SchemaInfo object

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        schema_name (str): <Catalog_Name>.<Schema_Name>

    Returns:
        catalog.SchemaInfo: Object containing information about UC Catalog
    """
    cat_name = schema_name.split(".")[-2]
    sch_name = schema_name.split(".")[-1]
    existing_schema = get_schema(client, schema_name=schema_name)
    existing_catalog = get_catalog(client, catalog_name=cat_name)
    if existing_schema is not None:
        print(f"Schema already exists: {schema_name}")
        return existing_schema
    elif existing_catalog is None:
        print(
            f"Attempting to create a schema in a catalog which does not exist: {schema_name}"
        )
        sys.exit(1)
    else:
        try:
            created_schema = client.schemas.create(
                name=sch_name, catalog_name=existing_catalog.name
            )
            print(f"Created schema: {schema_name}")
        except:
            print(f"Unable to create schema: {schema_name}")
            sys.exit(1)
        return created_schema


def get_schema(client: WorkspaceClient, schema_name: str) -> catalog.SchemaInfo:
    """Checks if Schema Exists & returns schemaInfo object if found

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        schema_name (str): <Catalog_Name>.<Schema_Name>

    Returns:
        catalog.SchemaInfo: Object containing information about UC Catalog
    """
    cat_name = schema_name.split(".")[-2]
    sch_name = schema_name.split(".")[-1]
    existing_catalog = get_catalog(client, catalog_name=cat_name)
    if existing_catalog is None:
        existing_schema = None
        print(f"The catalog for the specified schema does not exist: {schema_name}")
        return existing_schema
    else:
        all = client.schemas.list(catalog_name=existing_catalog.name)
        try:
            existing_schema = [x for x in all if x.name == sch_name][0]
        except:
            existing_schema = None
            print(f"Unable to locate schema: {schema_name}")
        return existing_schema


def delete_schema(client: WorkspaceClient, schema_name: str) -> None:
    """Checks if the Schema exists & deletes it if found

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        schema_name (str): <Catalog_Name>.<Schema_Name>
    """
    cat_name = schema_name.split(".")[-2]
    sch_name = schema_name.split(".")[-1]

    existing_catalog = get_catalog(client, cat_name)
    existing_schema = get_schema(client, schema_name)
    if existing_catalog is None:
        print(
            f"Attempting to delete a schema from a catalog that does not exist: {schema_name}"
        )
    elif existing_schema is None:
        print(f"Attempting to delete a schema that does not exist: {schema_name}")
    else:
        try:
            client.schemas.delete(full_name=existing_schema.full_name)
            print(f"Deleted schema: {schema_name}")
        except:
            print(f"Unable to delete schema: {schema_name}")


def create_storage_credential(
    client: WorkspaceClient,
    storage_credential_name: str,
    access_connector: str,
    comment: str = None,
) -> catalog.StorageCredentialInfo:
    """_summary_

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        storage_credential_name (str): Storage Credential Name
        access_connector (str): Access Connector
                '/subscriptions/<SubscriptionID>/resourcegroups/<ResourceGroup>/providers/Microsoft.Databricks/accessConnectors/<AccessConnectorName>'
        comment (str, optional): Comment. Defaults to None.

    Returns:
        catalog.StorageCredentialInfo: Object containing information about Storage Credential
    """
    existing_storage_credential = get_storage_credential(
        client, storage_credential_name
    )
    if existing_storage_credential is not None:
        print(f"Storage credential already exists: {storage_credential_name}")
        return existing_storage_credential
    else:
        try:
            created_storage_credential = client.storage_credentials.create(
                name=storage_credential_name,
                azure_managed_identity=AzureManagedIdentity(access_connector),
                comment=comment,
            )
            print(f"Created storage credential: {storage_credential_name}")
        except:
            print(f"Unable to create storage credential: {storage_credential_name}")
            sys.exit(1)
        return created_storage_credential


def get_storage_credential(
    client: WorkspaceClient, storage_credential_name: str
) -> catalog.StorageCredentialInfo:
    """Checks for existing storage credential & returns StorageCredentialInfo object if found

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        storage_credential_name (str): Storage Credential Name

    Returns:
        catalog.StorageCredentialInfo: Object containing information about Storage Credential
    """
    all = client.storage_credentials.list()
    try:
        existing_storage_credential = [
            x for x in all if x.name == storage_credential_name
        ][0]
    except:
        existing_storage_credential = None
        print(f"Unable to locate storage credential: {storage_credential_name}")
    return existing_storage_credential


def delete_storage_credential(
    client: WorkspaceClient, storage_credential_name: str
) -> None:
    """_summary_

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        storage_credential_name (str): _description_
    """
    existing_storage_credential = get_storage_credential(
        client, storage_credential_name
    )
    if existing_storage_credential is None:
        print(
            f"Attempting to delete storage credential which does not exist: {storage_credential_name}"
        )
    else:
        try:
            client.storage_credentials.delete(name=existing_storage_credential.name)
            print(f"Deleted storage credential: {storage_credential_name}")
        except:
            print(f"Unable to delete storage credential: {storage_credential_name}")


def create_external_location(
    client: WorkspaceClient,
    external_location_name: str,
    external_url: str,
    storage_credential_name: str,
    comment: str = None,
) -> catalog.ExternalLocationInfo:
    """_summary_

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        external_location_name (str): External Location Name
        external_url (str): External Location URL.
                'abfss://<container>@<storage_account>.dfs.core.windows.net/<path>'
        storage_credential_name (str): Storage Credential Name
        comment (str, optional): Comment. Defaults to None.

    Returns:
        catalog.ExternalLocationInfo: Object containing information about the External Location
    """
    existing_external_location = get_external_location(client, external_location_name)
    existing_storage_credential = get_storage_credential(
        client, storage_credential_name=storage_credential_name
    )
    if existing_external_location is not None:
        print(f"External location already exists: {external_location_name}")
        return existing_external_location
    elif existing_storage_credential is None:
        print(
            f"Attempting to create external location using a storage credential which does not exist: {storage_credential_name}"
        )
        sys.exit(1)
    else:
        try:
            created_external_location = client.external_locations.create(
                name=external_location_name,
                credential_name=existing_storage_credential.name,
                comment=comment,
                url=external_url,
            )
            print(f"Created external location: {external_location_name}")
        except:
            print(f"Unable to create external location: {external_location_name}")
            sys.exit(1)
        return created_external_location


def get_external_location(
    client: WorkspaceClient, external_location_name: str
) -> catalog.ExternalLocationInfo:
    """Checks if External Location exists and returns ExternalLocationInfo object if found

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        external_location_name (str): External Location Name

    Returns:
        catalog.ExternalLocationInfo: Object containing information about the External Location
    """
    all = client.external_locations.list()
    try:
        existing_external_location = [
            x for x in all if x.name == external_location_name
        ][0]
    except:
        existing_external_location = None
        print(f"Unable to locate external location: {external_location_name}")
    return existing_external_location


def delete_external_location(
    client: WorkspaceClient, external_location_name: str
) -> None:
    """Check if External Location exists & deletes if found

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        external_location_name (str): External Location Name
    """
    existing_external_location = get_external_location(client, external_location_name)
    if existing_external_location is None:
        print(
            f"Attempting to delete external location which does not exist: {external_location_name}"
        )
    else:
        try:
            client.external_locations.delete(name=existing_external_location.name)
            print(f"Deleted external location: {external_location_name}")
        except:
            print(f"Unable to delete external location: {external_location_name}")


def create_volume(
    client: WorkspaceClient,
    volume_name: str,
    external_location_name: str,
    comment: str = None,
) -> catalog.VolumeInfo:
    """Create Unity Catalog Volume & return VolumeInfo object

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        volume_name (str): UC Volume Name
        external_location_name (str): External Location Name
        comment (str, optional): Comment. Defaults to None.

    Returns:
        catalog.VolumeInfo: Object containing information about the UC Volume
    """
    vol_name = volume_name.split(".")[-1]
    sch_name = volume_name.split(".")[-2]
    cat_name = volume_name.split(".")[-3]
    existing_catalog = get_catalog(client, catalog_name=cat_name)
    existing_schema = get_schema(client, schema_name=f"{cat_name}.{sch_name}")
    existing_volume = get_volume(client, volume_name=volume_name)
    existing_external_location = get_external_location(
        client, external_location_name=external_location_name
    )
    if existing_volume is not None:
        print(f"Volume already exists: {volume_name}")
        return existing_volume
    elif existing_catalog is None:
        print(
            f"Attempting to create a volume in a catalog which does not exist: {volume_name}"
        )
        sys.exit(1)
    elif existing_schema is None:
        print(
            f"Attempting to create a volume in a schema which does not exist: {volume_name}"
        )
        sys.exit(1)
    elif existing_external_location is None:
        print(
            f"Attempting to create a volume with an external location which does not exist. Volume: {volume_name}, External Location: {external_location_name}"
        )
        sys.exit(1)
    else:
        try:
            created_volume = client.volumes.create(
                catalog_name=existing_catalog.name,
                schema_name=existing_schema.name,
                name=vol_name,
                storage_location=existing_external_location.url,
                comment=comment,
                volume_type=catalog.VolumeType.EXTERNAL,
            )
            print(f"Created volume: {volume_name}")
        except:
            print(f"Unable to create volume: {volume_name}")
            sys.exit(1)
        return created_volume


def get_volume(client: WorkspaceClient, volume_name: str) -> catalog.VolumeInfo:
    """Checks if UC Volume exists & returns VolumeInfo object if found

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        volume_name (str): Volume Name

    Returns:
        catalog.VolumeInfo: Object containing info about UC Volume object
    """
    cat_name = volume_name.split(".")[-3]
    sch_name = volume_name.split(".")[-2]
    vol_name = volume_name.split(".")[-1]
    existing_catalog = get_catalog(client, catalog_name=cat_name)
    existing_schema = get_schema(client, schema_name=f"{cat_name}.{sch_name}")
    if existing_catalog is None:
        existing_volume = None
        print(f"The catalog for the specified volume does not exist: {volume_name}")
        return existing_volume
    elif existing_schema is None:
        existing_volume = None
        print(f"The schema for the specified volume does not exist: {volume_name}")
        return existing_volume
    else:
        all = client.volumes.list(catalog_name=cat_name, schema_name=sch_name)
        try:
            existing_volume = [x for x in all if x.name == vol_name][0]
        except:
            existing_volume = None
            print(f"Unable to locate volume: {volume_name}")
        return existing_volume


def delete_volume(client: WorkspaceClient, volume_name: str) -> None:
    """Checks if Volume exists & deletes it if found

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        volume_name (str): UC Volume Name
    """
    existing_volume = get_volume(client, volume_name)
    if existing_volume is None:
        print(f"Attempting to delete volume which does not exist: {volume_name}")
    else:
        try:
            client.volumes.delete(full_name_arg=existing_volume.full_name)
            print(f"Deleted volume: {volume_name}")
        except:
            print(f"Unable to delete volume: {volume_name}")


def cleanup_uc_objects(
    client: WorkspaceClient,
    uc_catalogs: list[str],
    uc_schemas: list[str],
    uc_volumes: list[dict[str, str]],
    uc_external_locations: list[dict[str, str]],
    uc_storage_credentials: list[dict[str, str]],
) -> None:
    """Removes the UC objects

    Args:
        client (WorkspaceClient): Client for the workspace level Databricks REST API
        uc_catalogs (list[str]): List of UC Catalog Names
                [<catalog_1>, <catalog_2>, <catalog_n>]
        uc_schemas (list[str]): List of UC Schema Names
                [<catalog>.<schema_1>, <catalog>.<schema_2>, <catalog>.<schema_n>]
        uc_volumes (list[dict[str, str]]): List of dictionaries containing information about the UC Volume
                [{'volume_name': '<catalog>.<schema>.<volume>','external_location_name': '<external_location_name>','comment': '<comment>'}]
        uc_external_locations (list[dict[str, str]]): List of dictionaries containing information about the UC External Location
                [{'external_location_name': '<external_location_name>', 'external_url': 'abfss://<container>@<storage_account>.dfs.core.windows.net/<path>', 'storage_credential_name': '<storage_credential_name>', 'comment': 'comment'}]
        uc_storage_credentials (list[dict[str, str]]): List of dictionaries containing information about the UC Storage Credential
                [{'storage_credential_name': '<storage_credential_name>', 'access_connector': '/subscriptions/<subscriptionID>/resourcegroups/<resourceGroupName>/providers/Microsoft.Databricks/accessConnectors/<access_connector_name>', 'comment': 'comment'}]
    """
    for uc_volume in uc_volumes:
        delete_volume(client, volume_name=uc_volume["volume_name"])

    for uc_schema in uc_schemas:
        delete_schema(client, schema_name=uc_schema)

    for uc_catalog in uc_catalogs:
        delete_catalog(client, catalog_name=uc_catalog)

    for uc_external_location in uc_external_locations:
        delete_external_location(
            client,
            external_location_name=uc_external_location["external_location_name"],
        )

    for uc_storage_credential in uc_storage_credentials:
        delete_storage_credential(
            client,
            storage_credential_name=uc_storage_credential["storage_credential_name"],
        )


def create_uc_objects() -> None:
    """Creates the UC objects.
        If uc_cleanup is set to True in the config, then the objects will be removed
    """
    # Read "manage_uc_objects_config.ini" & parse configs
    uc_obj_config_path = pathlib.Path(__file__).parent / "manage_uc_objects_config.ini"
    uc_obj_config = configparser.ConfigParser()
    uc_obj_config.read(uc_obj_config_path)

    uc_cleanup = ast.literal_eval(uc_obj_config["options"]["uc_cleanup"])
    uc_catalogs = ast.literal_eval(uc_obj_config["uc_objects"]["uc_catalogs"])
    uc_schemas = ast.literal_eval(uc_obj_config["uc_objects"]["uc_schemas"])
    uc_volumes = ast.literal_eval(uc_obj_config["uc_objects"]["uc_volumes"])
    uc_external_locations = ast.literal_eval(
        uc_obj_config["uc_objects"]["uc_external_locations"]
    )
    uc_storage_credentials = ast.literal_eval(
        uc_obj_config["uc_objects"]["uc_storage_credentials"]
    )

    # Instantiate Databricks WorkspaceClient
    client = WorkspaceClient()

    # Removes ALL UC objects specified in "manage_uc_objects_config.ini" IF "uc_cleanup" is set to True
    if uc_cleanup == True:
        cleanup_uc_objects(
            client,
            uc_catalogs,
            uc_schemas,
            uc_volumes,
            uc_external_locations,
            uc_storage_credentials,
        )

    # Creates the UC objects specified in "manage_uc_objects_config.ini"
    else:
        for uc_storage_credential in uc_storage_credentials:
            create_storage_credential(
                client,
                storage_credential_name=uc_storage_credential["storage_credential_name"],
                access_connector=uc_storage_credential["access_connector"],
                comment=uc_storage_credential["comment"],
            )

        for uc_external_location in uc_external_locations:
            create_external_location(
                client,
                external_location_name=uc_external_location["external_location_name"],
                external_url=uc_external_location["external_url"],
                storage_credential_name=uc_external_location["storage_credential_name"],
                comment=uc_external_location["comment"],
            )

        for uc_catalog in uc_catalogs:
            create_catalog(client, uc_catalog)

        for uc_schema in uc_schemas:
            create_schema(client, uc_schema)

        for uc_volume in uc_volumes:
            create_volume(
                client,
                volume_name=uc_volume["volume_name"],
                external_location_name=uc_volume["external_location_name"],
                comment=uc_volume["comment"],
            )


if __name__ == "__main__":
    create_uc_objects()
