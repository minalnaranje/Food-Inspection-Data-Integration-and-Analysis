# Databricks notebook source
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Snowflake connection options
sfOptions = {
    "sfURL": "tq62449.canada-central.azure.snowflakecomputing.com",
    "sfUser": "BA_FOODINSP",
    "sfPassword": "BAFOODINSP",
    "sfDatabase": "FOODINSP_DB",
    "sfSchema": "FOODINSP_SCHEMA",
    "sfWarehouse": "FOODINSP_WH",
    "sfRole": "FOODINSP_BA"
}

# Read from Snowflake
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

# Combine and unify schema
combined_df = stg_chicago_df.select(
    "STREET_NUMBER",
    "STREET_DIRECTION",
    "STREET_NAME",
    "STREET_TYPE",
    "CITY",
    F.col("ZIP").alias("ZIP_CODE"),
    "LATITUDE",
    "LONGITUDE"
).union(
    stg_dallas_df.select(
        "STREET_NUMBER",
        "STREET_DIRECTION",
        "STREET_NAME",
        "STREET_TYPE",
        "CITY",
        "ZIP_CODE",
        "LATITUDE",
        "LONGITUDE"
    )
)

# Remove duplicates based on location-defining fields
combined_df = combined_df.dropDuplicates([
    "STREET_NUMBER", "STREET_DIRECTION", "STREET_NAME", "STREET_TYPE", 
    "CITY", "ZIP_CODE", "LATITUDE", "LONGITUDE"
])

# Add surrogate key
window_spec = Window.orderBy("STREET_NUMBER", "STREET_NAME", "CITY")
combined_df = combined_df.withColumn("LOCATION_ID_SK", F.row_number().over(window_spec))

# Add metadata columns
combined_df = combined_df.withColumn("JOB_ID", F.lit("job_001"))
combined_df = combined_df.withColumn("LOAD_DT", F.current_date())

# Write to Snowflake
combined_df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_LOCATION") \
    .mode("overwrite") \
    .save()

print("LOCATION_DIM table successfully loaded into Snowflake (without duplicates)!")


# COMMAND ----------

