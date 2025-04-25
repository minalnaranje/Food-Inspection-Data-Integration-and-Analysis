# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sfOptions = {
    "sfURL": "tq62449.canada-central.azure.snowflakecomputing.com",
    "sfUser": "BA_FOODINSP",
    "sfPassword": "BAFOODINSP",
    "sfDatabase": "FOODINSP_DB",
    "sfSchema": "FOODINSP_SCHEMA",
    "sfWarehouse": "FOODINSP_WH",
    "sfRole": "FOODINSP_BA"
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

# Select VIOLATION_DESCRIPTION from both sources
chicago_violations = stg_chicago_df.select(F.col("VIOLATIONS_NEW").alias("VIOLATION_DESCRIPTION"))
dallas_violations = stg_dallas_df.select(F.col("VIOLATION_DESCRIPTION"))

# Union both datasets
violation_df = chicago_violations.union(dallas_violations) \
                                 .dropna() \
                                 .dropDuplicates()

# Add surrogate key VIOLATION_ID_SK
window_spec = Window.orderBy("VIOLATION_DESCRIPTION")
violation_df = violation_df.withColumn("VIOLATION_ID_SK", F.row_number().over(window_spec))

# Add JOB_ID and LOAD_DT
violation_df = violation_df.withColumn("JOB_ID", F.lit(13)) \
                           .withColumn("LOAD_DT", F.current_date())

# Final column order
dim_violation_df = violation_df.select(
    "VIOLATION_ID_SK",
    "VIOLATION_DESCRIPTION",
    "JOB_ID",
    "LOAD_DT"
)

# Write to Snowflake
dim_violation_df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_VIOLATION") \
    .mode("overwrite") \
    .save()

print("✅ DIM_VIOLATION table successfully loaded into Snowflake!")
