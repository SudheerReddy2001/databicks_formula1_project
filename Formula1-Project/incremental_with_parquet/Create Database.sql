-- Databricks notebook source
-- MAGIC  %md
-- MAGIC  Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed_inc CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed_inc
LOCATION "/mnt/sudheerdatastorage1/incrementalload/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation_inc CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation_inc
LOCATION "/mnt/sudheerdatastorage1/incrementalload/presentation";
