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

# Combine inspection dates from both sources
date_df = stg_chicago_df.select("INSPECTION_DATE") \
    .union(
        stg_dallas_df.select("INSPECTION_DATE")
    ).dropna() \
     .dropDuplicates()

# Add derived columns: month, year, quarter
date_df = date_df.withColumn("INSPECTION_MONTH", F.month("INSPECTION_DATE")) \
                 .withColumn("INSPECTION_YEAR", F.year("INSPECTION_DATE")) \
                 .withColumn("INSPECTION_QUARTER", F.quarter("INSPECTION_DATE"))

# Add surrogate key DATE_SK
window_spec = Window.orderBy("INSPECTION_DATE")
date_df = date_df.withColumn("DATE_SK", F.row_number().over(window_spec))

# Add JOB_ID and LOAD_DT
date_df = date_df.withColumn("JOB_ID", F.lit(13)) \
                 .withColumn("LOAD_DT", F.current_date())

# Select final column order
dim_date_df = date_df.select(
    "DATE_SK",
    "INSPECTION_DATE",
    "INSPECTION_MONTH",
    "INSPECTION_YEAR",
    "INSPECTION_QUARTER",
    "JOB_ID",
    "LOAD_DT"
)

# Write to Snowflake
dim_date_df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_DATE") \
    .mode("overwrite") \
    .save()

print("✅ DIM_DATE table successfully loaded into Snowflake!")
