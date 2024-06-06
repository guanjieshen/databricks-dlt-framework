-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### [User Input Required] Read CSV Source with Autoloader.
-- MAGIC
-- MAGIC __Common data loading patterns__: https://docs.databricks.com/en/ingestion/auto-loader/patterns.html
-- MAGIC
-- MAGIC __Autoloader Options for CSV__: https://docs.databricks.com/en/ingestion/auto-loader/options.html#csv-options
-- MAGIC
-- MAGIC __Common Autoloader Options__: https://docs.databricks.com/en/ingestion/auto-loader/options.html#common-auto-loader-options
-- MAGIC
-- MAGIC __Schema Evolution Modes__: https://docs.databricks.com/en/ingestion/auto-loader/schema.html#how-does-auto-loader-schema-evolution-work
-- MAGIC

-- COMMAND ----------

CREATE
OR REFRESH TEMPORARY STREAMING TABLE autoloader_tmp_table AS
SELECT
  *
FROM
  STREAM READ_FILES(
    'dbfs:/databricks-datasets/nyctaxi/tripdata/yellow',
    format=>'csv',
    header=>'true',
    inferSchema=>'true',
    delimiter=>',',
    schemaEvolutionMode => 'addNewColumns',
    rescuedDataColumn => '_rescued_data'  
  );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [User Input Required] Transformations + Write Data to Unity Catalog

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE bronze_nyctaxi_tripdata_yellow
COMMENT "NYC Taxi Trip Records - Yellow Taxi Trip Records"
AS SELECT * FROM stream(LIVE.autoloader_tmp_table)

