# Databricks notebook source
# MAGIC %md
# MAGIC ### Import Helper Classes

# COMMAND ----------

from pipelines.shared_utils.writers import AutoloaderWriter, WriteMode, RefreshMode, TriggerMode
from pipelines.shared_utils.autoloader_helper import generated_autoloader_schema_path

# COMMAND ----------

# MAGIC %md
# MAGIC ### [User Input Required] Read CSV Source with Autoloader.
# MAGIC
# MAGIC Autoloader Options for CSV: https://docs.databricks.com/en/ingestion/auto-loader/options.html#csv-options

# COMMAND ----------

# [User Input Required] Set the ingest location.
ingest_location = "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow"

# Auto-Generate Schema Location Based on Ingest Location
autoloader_schema_location = generated_autoloader_schema_path(ingest_location)
print("Autoloader Schema Location: " +autoloader_schema_location)

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.header", "true")
    .option("cloudFiles.inferSchema","true")
    .option("cloudFiles.schemaLocation", autoloader_schema_location) # Required
    .load(ingest_location)
)

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### [User Input Required] Optional Transformations

# COMMAND ----------


df.createOrReplaceTempView("tmp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY VIEW output_table AS 
# MAGIC SELECT * FROM
# MAGIC tmp_view

# COMMAND ----------

# MAGIC %md
# MAGIC ### [User Input Required] Write Data

# COMMAND ----------

# [User Input Required] Target Location
catalog = ""
schema = ""
table = ""

# [User Input Required] Configs
write_mode = WriteMode.APPEND
refresh_mode = RefreshMode.INCREMENTAL
trigger_mode = TriggerMode.TRIGGERED

# COMMAND ----------

# Write data into the Data Lake and/or UC
csv_writer: AutoloaderWriter = AutoloaderWriter(df)
csv_writer.write_uc_external_table(
    uc_catalog=catalog,
    uc_schema=schema,
    uc_table=table,
    write_mode=write_mode,
    refresh_mode=refresh_mode,
    trigger_mode=trigger_mode,
)
