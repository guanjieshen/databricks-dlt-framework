# Databricks notebook source
# MAGIC %md
# MAGIC ### [User Input Required] Read CSV Source with Autoloader.
# MAGIC
# MAGIC __Common data loading patterns__: https://docs.databricks.com/en/ingestion/auto-loader/patterns.html
# MAGIC
# MAGIC __Autoloader Options for CSV__: https://docs.databricks.com/en/ingestion/auto-loader/options.html#csv-options
# MAGIC
# MAGIC __Common Autoloader Options__: https://docs.databricks.com/en/ingestion/auto-loader/options.html#common-auto-loader-options
# MAGIC
# MAGIC __Schema Evolution Modes__: https://docs.databricks.com/en/ingestion/auto-loader/schema.html#how-does-auto-loader-schema-evolution-work

# COMMAND ----------

import dlt

# [User Input Required] Set the input/output locations, metadata
input_location = "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow"
output_table = "bronze_nyctaxi_tripdata_yellow"
output_table_comments = "Bronze data for yellow NYC taxi trips"

# [User Input Required] Configure schema evolution and rescue data.
schema_evolution_mode = "addNewColumns"
rescue_data_column_name = "_rescued_data"


@dlt.table(name=f"{output_table}_autoloader", temporary = True)
def tmp():
  # [User Input Required] Configure Autoloader settings 
  df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.header", "true")
    .option("cloudFiles.inferSchema","true")
    .option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)
    .option("cloudFiles.rescuedDataColumn", rescue_data_column_name)
    # Add additional autoloader settings below
    .load(input_location)
  )
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Optional User Input Required] Transformations + Write Data to Unity Catalog

# COMMAND ----------

@dlt.table(name=output_table, comment = output_table_comments)
def t():
  # Read data from temporary autoloader table
  df = spark.readStream.table(f"live.{output_table}_autoloader")

  # [User Input Required] Optional Transformations
  # df = df.withColumn("trip_date", df["tpep_pickup_datetime"].cast("date")

  return df
