# Databricks notebook source
# MAGIC %sql
# MAGIC USE f1_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_presentation_inc.calculated_race_results;
# MAGIC CREATE TABLE f1_presentation_inc.calculated_race_results
# MAGIC USING parquet
# MAGIC AS
# MAGIC SELECT races.race_year,
# MAGIC        constructors.name AS team_name,
# MAGIC        drivers.name AS driver_name,
# MAGIC        results.position,
# MAGIC        results.points,
# MAGIC        11 - results.position AS calculated_points
# MAGIC   FROM f1_processed_inc.results 
# MAGIC   JOIN f1_processed_inc.drivers ON (results.driver_id = drivers.driver_id)
# MAGIC   JOIN f1_processed_inc.constructors ON (results.constructor_id = constructors.constructor_id)
# MAGIC   JOIN f1_processed_inc.races ON (results.race_id = races.race_id)
# MAGIC  WHERE results.position <= 10

# COMMAND ----------


spark.sql("SELECT * FROM f1_presentation_inc.calculated_race_results")
