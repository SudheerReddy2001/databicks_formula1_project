# Databricks notebook source
# Databricks notebook source
raw_database="par_f1_raw_inc"
process_database="par_f1_processed_inc"
presentation_database="par_f1_final_inc"

# COMMAND ----------

container_name="incrementalload"

# COMMAND ----------

raw_path="/mnt/sudheerdatastorage1/incrementalload/raw"
process_path="/mnt/sudheerdatastorage1/incrementalload/processed"
presentation_path="/mnt/sudheerdatastorage1/incrementalload/presentation"
