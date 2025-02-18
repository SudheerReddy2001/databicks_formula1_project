-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

DROP TABLE IF EXISTS deltalake_presentation.calculated_race_results;
CREATE TABLE deltalake_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
  FROM results 
  JOIN deltalake_processed.drivers ON (results.driver_id = drivers.driver_id)
  JOIN deltalake_processed.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN deltalake_processed.races ON (results.race_id = races.race_id)
 WHERE results.position <= 10

-- COMMAND ----------

SELECT * FROM deltalake_presentation.calculated_race_results
