# Databricks notebook source
# MAGIC  %md
# MAGIC Ingest lap_times folder

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/Function_Time"

# COMMAND ----------

# MAGIC  %md
# MAGIC  Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC  %md
# MAGIC - Rename columns and add new columns
# MAGIC  Rename driverId and raceId
# MAGIC  Add ingestion_date with current timestamp
# MAGIC

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df,v_file_date)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = lap_times_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id")  \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC  %md
# MAGIC  Write to output to processed container in parquet format

# COMMAND ----------

overwrite_partition(final_df, 'f1_processed_inc', 'lap_times', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
