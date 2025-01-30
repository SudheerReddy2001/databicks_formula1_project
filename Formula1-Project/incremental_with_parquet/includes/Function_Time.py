# Databricks notebook source
def re_arrange_partition_column(input_df, partition_column):
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")


# COMMAND ----------

# Databricks notebook source
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

def add_ingestion_date(df,filename):
    output_df=df.withColumn("ingestion_date",current_timestamp())\
        .withColumn("filename",lit(filename))
    return output_df

# COMMAND ----------

source_point="table"

# COMMAND ----------

def updatepartitioncolumn(df,partition_column):
    columns=[]
    for column in df.columns:
        if column !=partition_column:
            columns.append(column)
    columns.append(partition_column)
    out_df=df.select(columns)

    return out_df 

# COMMAND ----------

def merge_table(df, db_name, table_name, process_path, partition_condition):
    from delta.tables import DeltaTable

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    if source_point == "adls":
        
        df.write.format("parquet").partitionBy(partition_condition).mode("overwrite").save(f"{process_path}/{table_name}")
    elif source_point == "table":
        
        if spark.catalog.tableExists(f"{db_name}.{table_name}"):
           
            df = updatepartitioncolumn(df, partition_condition)

            
            df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
            print("Table partitions updated.")
        else:
            
            df.write.partitionBy(partition_condition).format("parquet").mode("overwrite") \
                .option("path", f"abfss://{container_name}@sudheerdatastorage1.dfs.core.windows.net/{process_path}/{table_name}") \
                .saveAsTable(f"{db_name}.{table_name}")
            print("New table created.")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------


