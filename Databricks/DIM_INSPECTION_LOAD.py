# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# Snowflake connection options
sfOptions = {
    "sfURL": "tq62449.canada-central.azure.snowflakecomputing.com",
    "sfUser": "BA_FOODINSP",
    "sfPassword": "BAFOODINSP",
    "sfDatabase": "FOODINSP_DB",
    "sfSchema": "FOODINSP_SCHEMA",
    "sfWarehouse": "FOODINSP_WH",
    "sfRole": "FOODINSP_BA",
    "dbtable": "STG_CHICAGO"
}
# Read from STG_CHICAGO and STG_DALLAS
stg_chicago_df = spark.read \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "STG_CHICAGO") \
    .load()

stg_dallas_df = spark.read \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "STG_DALLAS") \
    .load()

# Select INSPECTION_TYPE from both sources
inspection_types_df = stg_chicago_df.select("INSPECTION_TYPE") \
    .union(
        stg_dallas_df.select("INSPECTION_TYPE")
    ).dropna() \
     .dropDuplicates()

# Add surrogate key
window_spec = Window.orderBy("INSPECTION_TYPE")
dim_inspection_df = inspection_types_df.withColumn("INSPECTION_TYPE_SK", F.row_number().over(window_spec))

# Add JOB_ID and LOAD_DT
dim_inspection_df = dim_inspection_df.withColumn("JOB_ID", F.lit("job_001")) \
                                     .withColumn("LOAD_DT", F.current_date())

# Optional: rearrange columns
dim_inspection_df = dim_inspection_df.select(
    "INSPECTION_TYPE_SK",
    "INSPECTION_TYPE",
    "JOB_ID",
    "LOAD_DT"
)

# Write to Snowflake
dim_inspection_df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_INSPECTION") \
    .mode("overwrite") \
    .save()

print("✅ DIM_INSPECTION table successfully loaded into Snowflake!")
