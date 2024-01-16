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
GRANT ALL PRIVILEGES ON STORAGE CREDENTIAL `ubdatabrickspocdevtestsc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the UC Volume of dev catalog bronze schema
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_bronze_volume_ext_loc` URL 'abfss://databricks-poc@ubsadatabrickspoc2.dfs.core.windows.net/umpqua_poc/volumes/bronze'
    WITH (CREDENTIAL `ubdatabrickspocdevtestsc`)
    COMMENT 'External Location for the umpqua_poc_dev.bronze_data.bronze_volume';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `umpqua_poc_dev_bronze_volume_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the UC Volume of dev catalog silver schema
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_silver_volume_ext_loc` URL 'abfss://databricks-poc@ubsadatabrickspoc2.dfs.core.windows.net/umpqua_poc/volumes/silver'
    WITH (CREDENTIAL `ubdatabrickspocdevtestsc`)
    COMMENT 'External Location for the umpqua_poc_dev.silver_data.silver_volume';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `umpqua_poc_dev_silver_volume_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the UC Volume of dev catalog gold schema
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_gold_volume_ext_loc` URL 'abfss://databricks-poc@ubsadatabrickspoc2.dfs.core.windows.net/umpqua_poc/volumes/gold'
    WITH (CREDENTIAL `ubdatabrickspocdevtestsc`)
    COMMENT 'External Location for the umpqua_poc_dev.gold_data.gold_volume';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `umpqua_poc_dev_gold_volume_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for the gold schema in the dev catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_landing_zone_ext_loc` URL 'abfss://databricks-poc@ubsadatabrickspoc2.dfs.core.windows.net/umpqua_poc/landing_zone'
    WITH (CREDENTIAL `ubdatabrickspocdevtestsc`)
    COMMENT 'External Location for the dev data landing zone';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `umpqua_poc_dev_landing_zone_ext_loc` TO `umpqua_poc_dev_group`;


-- Create external location to be used for any other purpose in the dev catalog
CREATE EXTERNAL LOCATION IF NOT EXISTS `umpqua_poc_dev_playground_ext_loc` URL 'abfss://databricks-poc@ubsadatabrickspoc2.dfs.core.windows.net/umpqua_poc/playground'
    WITH (CREDENTIAL `ubdatabrickspocdevtestsc`)
    COMMENT 'External Location for the umpqua_poc_dev playground';
-- Grant permission to user, group, or service principal to browse, read, & write files located in the external location & create managed storage and external volumes using the external location
GRANT ALL PRIVILEGES ON EXTERNAL LOCATION `umpqua_poc_dev_playground_ext_loc` TO `umpqua_poc_dev_group`;


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

-- Grant "data editor" permissions to to user, group, or service principal on Catalog
GRANT ALL PRIVILEGES ON CATALOG `umpqua_poc_dev` TO `umpqua_poc_dev_group`;

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
  LOCATION 'abfss://databricks-poc@ubsadatabrickspoc2.dfs.core.windows.net/umpqua_poc/volumes/bronze'
  COMMENT 'External volume for the DEV Umpqua POC bronze layer';

CREATE EXTERNAL VOLUME IF NOT EXISTS umpqua_poc_dev.silver_data.silver_volume
  LOCATION 'abfss://databricks-poc@ubsadatabrickspoc2.dfs.core.windows.net/umpqua_poc/volumes/silver'
  COMMENT 'External volume for the DEV Umpqua POC silver layer';

CREATE EXTERNAL VOLUME IF NOT EXISTS umpqua_poc_dev.gold_data.gold_volume
  LOCATION 'abfss://databricks-poc@ubsadatabrickspoc2.dfs.core.windows.net/umpqua_poc/volumes/gold'
  COMMENT 'External volume for the DEV Umpqua POC gold layer';


-- COMMAND ----------

