def uc_object_config(
    project_name, storage_account_name, storage_container_name, storage_credential_name
):
    project_config = dict()
    project_config['catalogs'] = [
        {
            'catalog': f'{project_name}_dev',
        }
    ]
    project_config['schemas'] = [
        {
            'catalog': f'{project_name}_dev',
            'schema': 'bronze_data',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/tables/bronze',
        },
        {
            'catalog': f'{project_name}_dev',
            'schema': 'silver_data',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/tables/silver',
        },
        {
            'catalog': f'{project_name}_dev',
            'schema': 'gold_data',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/tables/gold',
        },
    ]
    project_config['volumes'] = [
        {
            'catalog': f'{project_name}_dev',
            'schema': 'bronze_data',
            'volume': 'bronze_volume',
        },
        {
            'catalog': f'{project_name}_dev',
            'schema': 'silver_data',
            'volume': 'silver_volume',
        },
        {
            'catalog': f'{project_name}_dev',
            'schema': 'gold_data',
            'volume': 'gold_volume',
        },
    ]

    project_config['locations'] = [
        {
            'name': f'{project_name}_dev_bronze_schema_ext_loc',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/tables/bronze',
            'credential': f'{storage_credential_name}',
            'comment': f'External location for the bronze schema of the dev catalog for project {project_name}',
        },
        {
            'name': f'{project_name}_dev_silver_schema_ext_loc',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/tables/silver',
            'credential': f'{storage_credential_name}',
            'comment': f'External location for the silver schema of the dev catalog for project {project_name}',
        },
        {
            'name': f'{project_name}_dev_gold_schema_ext_loc',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/tables/gold',
            'credential': f'{storage_credential_name}',
            'comment': f'External location for the gold schema of the dev catalog for project {project_name}',
        },
        {
            'name': f'{project_name}_dev_bronze_volume_ext_loc',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/volumes/bronze',
            'credential': f'{storage_credential_name}',
            'comment': f'External location for the bronze volume of the dev catalog for project {project_name}',
        },
        {
            'name': f'{project_name}_dev_silver_volume_ext_loc',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/volumes/silver',
            'credential': f'{storage_credential_name}',
            'comment': f'External location for the silver volume of the dev catalog for project {project_name}',
        },
        {
            'name': f'{project_name}_dev_gold_volume_ext_loc',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/volumes/gold',
            'credential': f'{storage_credential_name}',
            'comment': f'External location for the gold volume of the dev catalog for project {project_name}',
        },
        {
            'name': f'{project_name}_dev_landing_zone_ext_loc',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/landing_zone',
            'credential': f'{storage_credential_name}',
            'comment': f'External location for the landing zone of the dev catalog for project {project_name}',
        },
        {
            'name': f'{project_name}_dev_playground_ext_loc',
            'location': f'abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/{project_name}/playground',
            'credential': f'{storage_credential_name}',
            'comment': f'External location for the playground of the dev catalog for project {project_name}',
        },
    ]
    return project_config


def uc_permissions_config(project_name):
    permissions = [
        {
            'grants': [
                'USE CATALOG',
                'USE SCHEMA',
                'APPLY TAG',
                'BROWSE',
                'EXECUTE',
                'REFRESH',
                'MODIFY',
                'READ VOLUME',
                'SELECT',
                'WRITE VOLUME',
                'CREATE FUNCTION',
                'CREATE MATERIALIZED VIEW',
                'CREATE MODEL',
                'CREATE SCHEMA',
                'CREATE TABLE',
                'CREATE VOLUME',
            ],
            'object_type': 'CATALOG',
            'object_name': f'{project_name}_dev',
            'principal': [f'{project_name}_dev_group'],
        },
        {
            'grants': [
                'BROWSE',
                'READ FILES',
                'WRITE FILES',
                'CREATE MANAGED STORAGE',
            ],
            'object_type': 'EXTERNAL LOCATION',
            'object_name': f'{project_name}_dev_bronze_schema_ext_loc',
            'principal': [f'{project_name}_dev_group'],
        },
        {
            'grants': [
                'BROWSE',
                'READ FILES',
                'WRITE FILES',
                'CREATE MANAGED STORAGE',
            ],
            'object_type': 'EXTERNAL LOCATION',
            'object_name': f'{project_name}_dev_silver_schema_ext_loc',
            'principal': [f'{project_name}_dev_group'],
        },
        {
            'grants': [
                'BROWSE',
                'READ FILES',
                'WRITE FILES',
                'CREATE MANAGED STORAGE',
            ],
            'object_type': 'EXTERNAL LOCATION',
            'object_name': f'{project_name}_dev_gold_schema_ext_loc',
            'principal': [f'{project_name}_dev_group'],
        },
        {
            'grants': [
                'BROWSE',
                'READ FILES',
                'WRITE FILES',
                'CREATE EXTERNAL VOLUME',
                'CREATE MANAGED STORAGE',
            ],
            'object_type': 'EXTERNAL LOCATION',
            'object_name': f'{project_name}_dev_bronze_volume_ext_loc',
            'principal': [f'{project_name}_dev_group'],
        },
        {
            'grants': [
                'BROWSE',
                'READ FILES',
                'WRITE FILES',
                'CREATE EXTERNAL VOLUME',
                'CREATE MANAGED STORAGE',
            ],
            'object_type': 'EXTERNAL LOCATION',
            'object_name': f'{project_name}_dev_silver_volume_ext_loc',
            'principal': [f'{project_name}_dev_group'],
        },
        {
            'grants': [
                'BROWSE',
                'READ FILES',
                'WRITE FILES',
                'CREATE EXTERNAL VOLUME',
                'CREATE MANAGED STORAGE',
            ],
            'object_type': 'EXTERNAL LOCATION',
            'object_name': f'{project_name}_dev_gold_volume_ext_loc',
            'principal': [f'{project_name}_dev_group'],
        },
        {
            'grants': [
                'BROWSE',
                'READ FILES',
                'WRITE FILES',
                'CREATE EXTERNAL VOLUME',
                'CREATE MANAGED STORAGE',
            ],
            'object_type': 'EXTERNAL LOCATION',
            'object_name': f'{project_name}_dev_landing_zone_ext_loc',
            'principal': [f'{project_name}_dev_group'],
        },
        {
            'grants': [
                'BROWSE',
                'READ FILES',
                'WRITE FILES',
                'CREATE EXTERNAL VOLUME',
                'CREATE MANAGED STORAGE',
            ],
            'object_type': 'EXTERNAL LOCATION',
            'object_name': f'{project_name}_dev_playground_ext_loc',
            'principal': [f'{project_name}_dev_group'],
        },
    ]
    return permissions


if __name__ == '__main__':
    project_name = 'umpqua_poc'
    storage_account_name = 'oneenvadls'
    storage_container_name = 'umpquapocdev'
    storage_credential_name = (
        '121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101'
    )
    project_config = uc_object_config(
        project_name,
        storage_account_name,
        storage_container_name,
        storage_credential_name,
    )

    permissions_config = uc_permissions_config(project_name)
