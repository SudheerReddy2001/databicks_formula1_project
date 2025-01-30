# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")
lap_times_df = spark.read.parquet(f"{processed_folder_path}/lap_times")
pit_stops_df = spark.read.parquet(f"{processed_folder_path}/pit_stops")
qualifying_df = spark.read.parquet(f"{processed_folder_path}/qualifying")
races_df = spark.read.parquet(f"{processed_folder_path}/races")
results_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# Read drivers DataFrame
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")

# Read constructors DataFrame
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")

# Read circuits DataFrame
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .withColumnRenamed("location", "circuit_location")


# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

race_results_df = results_df.join(
    races_df.alias("races"), results_df.race_id == races_df.race_id, "inner"
).join(
    circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner"
).join(
    drivers_df, results_df.driver_id == drivers_df.driver_id, "inner"
).join(
    constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner"
)

final_df = race_results_df.select(
    col("races.race_year").alias("race_year"),
    col("races.name").alias("race_name"), 
    col("races.race_timestamp").alias("race_date"),
    col("circuit_location").alias("circuit_location"),
    col("driver_name").alias("driver_name"),
    col("driver_number").alias("driver_number"),
    col("driver_nationality").alias("driver_nationality"),
    col("team").alias("team"),
    col("grid").alias("grid"),
    col("fastest_lap").alias("fastest_lap"),
    col("time").alias("race_time"),
    col("points").alias("points"),
    current_timestamp().alias("created_date")
)

filtered_df = final_df.filter(
    (col("race_year") == 2020) & (col("race_name") == "Abu Dhabi Grand Prix")
).orderBy(col("points").desc())

display(filtered_df)
filtered_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/results")

# COMMAND ----------

race_results_df = results_df.alias("results").join(
    races_df.alias("races"), col("results.race_id") == col("races.race_id"), "inner"
).join(
    circuits_df.alias("circuits"), col("races.circuit_id") == col("circuits.circuit_id"), "inner"
).join(
    drivers_df.alias("drivers"), col("results.driver_id") == col("drivers.driver_id"), "inner"
).join(
    constructors_df.alias("constructors"), col("results.constructor_id") == col("constructors.constructor_id"), "inner"
)


# COMMAND ----------

from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
 
 
window_spec = Window.partitionBy("race_year").orderBy(col("total_points").desc())
 
 
driver_standings_df = race_results_df.groupBy(
    col("drivers.driver_id").alias("driver_id"),
    col("races.race_year").alias("race_year"),
    col("drivers.driver_name").alias("driver_name"),
    col("constructors.team").alias("team"),
    col("drivers.driver_nationality").alias("driver_nationality")
).agg(
    sum(col("results.points")).alias("total_points")
).withColumn(
    "rank", dense_rank().over(window_spec)  
)
display(driver_standings_df)
 
driver_standings_output_path = f"{presentation_folder_path}/driver_standings"
driver_standings_df.write.mode("overwrite").parquet(driver_standings_output_path)
 
 

# COMMAND ----------

# Window specification for dense rank
window_spec = Window.partitionBy("race_year").orderBy(col("total_points").desc())
 
# Calculate constructor standings
constructor_standings_df = race_results_df.groupBy(
    col("constructors.constructor_id").alias("constructor_id"),
    col("races.race_year").alias("race_year"),
    col("constructors.team").alias("team")
).agg(
    sum(col("results.points")).alias("total_points")
).withColumn(
    "rank", dense_rank().over(window_spec)
)
display(constructor_standings_df)
# Save constructor standings
constructor_standings_output_path = f"{presentation_folder_path}/constructor_standings"
constructor_standings_df.write.mode("overwrite").parquet(constructor_standings_output_path)
 
 
