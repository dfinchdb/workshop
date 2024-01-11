-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Manage UC Objects Using SQL Commands
-- MAGIC #### The following commands can be used to create the basic Unity Catalog objects required for a project & to set permissions on those objects:
-- MAGIC   - Storage Credential
-- MAGIC   - External Locations
-- MAGIC   - Catalog
-- MAGIC   - Schemas
-- MAGIC   - Volumes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Storage Credentials:
-- MAGIC ###### Storage credentials can be created using the UI by following the directions below:
-- MAGIC - Navigate to the Catalog Page -> External Data -> Storage Credentials -> Create credential
-- MAGIC - Create a storage credential using the following inputs:
-- MAGIC   - Credential Type: Azure Managed Identity
-- MAGIC   - Storage Credential Name: umpqua_poc_sc
-- MAGIC   - Access Connector ID: '/subscriptions/\<subscriptionID>/resourcegroups/\<resourceGroupName>/providers/Microsoft.Databricks/accessConnectors/\<accessConnectorName>'
-- MAGIC   - Comment: Umpqua POC storage credential
-- MAGIC ###### For further information visit: 
-- MAGIC - https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-storage-credentials

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### External Locations:
-- MAGIC ###### Storage credentials can be created using SQL commands & the inputs below:
-- MAGIC - external_location_name: umpqua_poc_bronze_ext_loc 
-- MAGIC - external_url: abfss://\<container>@\<storage_account>.dfs.core.windows.net/\<path>
-- MAGIC - storage_credential_name: umpqua_poc_sc (Created in previous step)
-- MAGIC - comment: Umpqua POC bronze external location
-- MAGIC ###### For further information visit: 
-- MAGIC - https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-external-locations

-- COMMAND ----------

-- EXTERNAL LOCATIONS FOR THE DEV CATALOG

-- Grant permission to user, group, or service principal to create external locations using the dev storage credential
GRANT ALL PRIVILEGES ON STORAGE CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the UC Volume of dev catalog bronze schema
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_bronze_volume_ext_loc` URL 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/bronze'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_dev.bronze_data.bronze_volume';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_dev_bronze_volume_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the UC Volume of dev catalog silver schema
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_silver_volume_ext_loc` URL 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/silver'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_dev.silver_data.silver_volume';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_dev_silver_volume_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the UC Volume of dev catalog gold schema
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_gold_volume_ext_loc` URL 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/gold'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_dev.gold_data.gold_volume';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_dev_gold_volume_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the bronze schema in the dev catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_bronze_schema_ext_loc` URL 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/tables/bronze'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_dev.bronze_data';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_dev_bronze_schema_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the silver schema in the dev catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_silver_schema_ext_loc` URL 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/tables/silver'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_dev.silver_data';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_dev_silver_schema_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the gold schema in the dev catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_gold_schema_ext_loc` URL 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/tables/gold'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_dev.gold_data';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_dev_gold_schema_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the gold schema in the dev catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_landing_zone_ext_loc` URL 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/landing_zone'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the dev data landing zone';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_dev_landing_zone_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for any other purpose in the dev catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_playground_ext_loc` URL 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/playground'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_dev playground';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_dev_playground_ext_loc` TO `umpqua_poc_dev_group`;

-- COMMAND ----------

-- EXTERNAL LOCATIONS FOR THE PRD CATALOG

-- Grant permission to user, group, or service principal to create external locations using the PRD storage credential
GRANT ALL PRIVILEGES ON STORAGE CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101` TO `umpqua_poc_prd_group`;


-- Create external location to be used for the Unity Catalog Volume in the Bronze Schema of the PRD Catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_prd_bronze_volume_ext_loc` URL 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/bronze'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_prd.bronze_data.bronze_volume';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_prd_bronze_volume_ext_loc` TO `umpqua_poc_prd_group`;


-- Create external location to be used for the Unity Catalog Volume in the Silver Schema of the PRD Catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_prd_silver_volume_ext_loc` URL 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/silver'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_prd.silver_data.silver_volume';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_prd_silver_volume_ext_loc` TO `umpqua_poc_prd_group`;


-- Create external location to be used for the Unity Catalog Volume in the Gold Schema of the PRD Catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_prd_gold_volume_ext_loc` URL 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/gold'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_prd.gold_data.gold_volume';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_prd_gold_volume_ext_loc` TO `umpqua_poc_prd_group`;


-- Create external location to be used for the Bronze Schema of the PRD Catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_prd_bronze_schema_ext_loc` URL 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/tables/bronze'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_prd.bronze_data';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_prd_bronze_schema_ext_loc` TO `umpqua_poc_prd_group`;


-- Create external location to be used for the Silver Schema of the PRD Catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_prd_silver_schema_ext_loc` URL 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/tables/silver'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_prd.silver_data';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_prd_silver_schema_ext_loc` TO `umpqua_poc_prd_group`;


-- Create external location to be used for the Gold Schema of the PRD Catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_prd_gold_schema_ext_loc` URL 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/tables/gold'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_prd.gold_data';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_prd_gold_schema_ext_loc` TO `umpqua_poc_prd_group`;


-- Create external location to be used for the gold schema in the dev catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_prd_landing_zone_ext_loc` URL 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/landing_zone'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the prd data landing zone';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_prd_landing_zone_ext_loc` TO `umpqua_poc_prd_group`;

-- Create external location to be used for any other purpose in the prd catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_prd_playground_ext_loc` URL 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/playground'
    WITH (CREDENTIAL `121ccfbf-3d4f-4744-87f5-36b1c921c903-storage-credential-1703096015101`)
    COMMENT 'External Location for the umpqua_poc_prd playground';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT BROWSE, READ FILES, WRITE FILES, CREATE EXTERNAL VOLUME, CREATE MANAGED STORAGE ON EXTERNAL LOCATION `umpqua_poc_prd_playground_ext_loc` TO `umpqua_poc_prd_group`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Catalogs:
-- MAGIC ###### Catalogs can be created using SQL commands & the inputs below:
-- MAGIC - catalog_name: umpqua_poc 
-- MAGIC ###### For further information visit: 
-- MAGIC - https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-catalog

-- COMMAND ----------

-- Create the DEV Catalog

-- Create Catalog if it does not exist
CREATE CATALOG IF NOT EXISTS umpqua_poc_dev;

-- Enable predictive optimization at the catalog level
ALTER CATALOG umpqua_poc_dev ENABLE PREDICTIVE OPTIMIZATION;

-- Grant "data editor" permissions to to user, group, or service principal on Catalog
GRANT USE CATALOG, USE SCHEMA, APPLY TAG, BROWSE, EXECUTE, REFRESH, MODIFY, READ VOLUME, SELECT, WRITE VOLUME, CREATE FUNCTION, CREATE MATERIALIZED VIEW, CREATE MODEL, CREATE SCHEMA, CREATE TABLE, CREATE VOLUME ON CATALOG `umpqua_poc_dev` TO `umpqua_poc_dev_group`;

-- COMMAND ----------

-- Create the PRD Catalog

-- Create Catalog if it does not exist
CREATE CATALOG IF NOT EXISTS umpqua_poc_prd;

-- Enable predictive optimization at the catalog level
ALTER CATALOG umpqua_poc_prd ENABLE PREDICTIVE OPTIMIZATION;

-- Grant "data editor" permissions to to user, group, or service principal on Catalog
GRANT USE CATALOG, USE SCHEMA, APPLY TAG, BROWSE, EXECUTE, REFRESH, MODIFY, READ VOLUME, SELECT, WRITE VOLUME, CREATE FUNCTION, CREATE MATERIALIZED VIEW, CREATE MODEL, CREATE SCHEMA, CREATE TABLE, CREATE VOLUME ON CATALOG `umpqua_poc_prd` TO `umpqua_poc_prd_group`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Schemas:
-- MAGIC ###### Schemas can be created using SQL commands & the inputs below:
-- MAGIC - catalog_name: umpqua_poc
-- MAGIC - schema_name: bronze_data
-- MAGIC ###### For further information visit: 
-- MAGIC - https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-schema

-- COMMAND ----------

-- Create the DEV Schemas

-- Create Schema if it does not exist
CREATE SCHEMA IF NOT EXISTS umpqua_poc_dev.bronze_data;

-- Create Schema if it does not exist
CREATE SCHEMA IF NOT EXISTS umpqua_poc_dev.silver_data;

-- Create Schema if it does not exist
CREATE SCHEMA IF NOT EXISTS umpqua_poc_dev.gold_data;


-- COMMAND ----------

-- Create the PRD Schemas

-- Create Schema if it does not exist
CREATE SCHEMA IF NOT EXISTS umpqua_poc_prd.bronze_data;

-- Create Schema if it does not exist
CREATE SCHEMA IF NOT EXISTS umpqua_poc_prd.silver_data;

-- Create Schema if it does not exist
CREATE SCHEMA IF NOT EXISTS umpqua_poc_prd.gold_data;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Volumes:
-- MAGIC ###### Volumes can be created using SQL commands & the inputs below:
-- MAGIC - catalog_name: umpqua_poc
-- MAGIC - schema_name: bronze_data
-- MAGIC ###### For further information visit: 
-- MAGIC - https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-volume

-- COMMAND ----------

-- Create the DEV Volumes

CREATE EXTERNAL VOLUME IF NOT EXISTS umpqua_poc_dev.bronze_data.bronze_volume
  LOCATION 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/bronze'
  COMMENT 'External volume for the DEV Umpqua POC bronze layer';

CREATE EXTERNAL VOLUME IF NOT EXISTS umpqua_poc_dev.silver_data.silver_volume
  LOCATION 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/silver'
  COMMENT 'External volume for the DEV Umpqua POC silver layer';

CREATE EXTERNAL VOLUME IF NOT EXISTS umpqua_poc_dev.gold_data.gold_volume
  LOCATION 'abfss://umpquapocdev@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/gold'
  COMMENT 'External volume for the DEV Umpqua POC gold layer';

-- COMMAND ----------

-- Create the PRD Volumes

CREATE EXTERNAL VOLUME IF NOT EXISTS umpqua_poc_prd.bronze_data.bronze_volume
  LOCATION 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/bronze'
  COMMENT 'External volume for the PRD Umpqua POC bronze layer';

CREATE EXTERNAL VOLUME IF NOT EXISTS umpqua_poc_prd.silver_data.silver_volume
  LOCATION 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/silver'
  COMMENT 'External volume for the PRD Umpqua POC silver layer';

CREATE EXTERNAL VOLUME IF NOT EXISTS umpqua_poc_prd.gold_data.gold_volume
  LOCATION 'abfss://umpquapocprd@ubsadatabrickspoc.dfs.core.windows.net/umpqua_poc/volumes/gold'
  COMMENT 'External volume for the PRD Umpqua POC gold layer';

-- COMMAND ----------

