# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, sum, dense_rank
from pyspark.sql.window import Window


circuits_df = spark.sql("SELECT *, location AS circuit_location, name AS circuit_name FROM f1_processed.circuits").drop("location").drop("name")
drivers_df = spark.sql("SELECT *, number AS driver_number, name AS driver_name, nationality AS driver_nationality FROM f1_processed.drivers").drop("number").drop("name").drop("nationality")
constructors_df = spark.sql("SELECT *, name AS team FROM f1_processed.constructors").drop("name")
races_df = spark.sql("SELECT *, name AS race_name FROM f1_processed.races").drop("name")
results_df = spark.sql("SELECT * FROM f1_processed.results")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Race-result

# COMMAND ----------

race_results_df = results_df.join(
    races_df, "race_id"
).join(
    circuits_df, "circuit_id"
).join(
    drivers_df, "driver_id"
).join(
    constructors_df, "constructor_id"
)

# COMMAND ----------

race_year = 2020
race_name = "Abu Dhabi Grand Prix"

filtered_df = race_results_df.filter(
    (col("race_year") == race_year) & (col("race_name") == race_name)
).select(
    col("race_year"),
    col("race_name"),
    col("race_timestamp").alias("race_date"),
    col("circuit_location"),  
    col("driver_name"),
    col("driver_number"),
    col("driver_nationality"),
    col("team"),
    col("grid"),
    col("fastest_lap"),
    col("time").alias("race_time"),
    col("points"),
    current_timestamp().alias("created_date")
).orderBy(col("points").desc())


display(filtered_df)

# COMMAND ----------

filtered_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Driver-standings

# COMMAND ----------

driver_standings_df = race_results_df.groupBy(
    col("driver_id"),
    col("race_year"),
    col("driver_name"),
    col("team"),
    col("driver_nationality")
).agg(
    sum(col("points")).alias("total_points")
).withColumn(
    "rank", dense_rank().over(Window.partitionBy("race_year").orderBy(col("total_points").desc()))
)

display(driver_standings_df)
driver_standings_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Constructor-standings

# COMMAND ----------

constructor_standings_df = race_results_df.groupBy(
    col("constructor_id"),
    col("race_year"),
    col("team")
).agg(
    sum(col("points")).alias("total_points")
).withColumn(
    "rank", dense_rank().over(Window.partitionBy("race_year").orderBy(col("total_points").desc()))
)
display(constructor_standings_df)

constructor_standings_df.write.mode("overwrite").format("delta").saveAsTable("f1_presentation.constructor_standings")

