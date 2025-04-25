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

# Select necessary columns from each source (don't rename at this point)
chicago_df = stg_chicago_df.select("DBA_NAME", "AKA_NAME", "FACILITY_TYPE", "LICENSE")
dallas_df = stg_dallas_df.select("RESTAURANT_NAME", "FACILITY_TYPE")

# Align columns while combining
chicago_aligned = chicago_df.withColumnRenamed("DBA_NAME", "BUSINESS_NAME") \
                            .withColumnRenamed("FACILITY_TYPE", "BUSINESS_TYPE")

dallas_aligned = dallas_df.withColumnRenamed("RESTAURANT_NAME", "BUSINESS_NAME") \
                          .withColumnRenamed("FACILITY_TYPE", "BUSINESS_TYPE") \
                          .withColumn("AKA_NAME", F.lit(None).cast("string")) \
                          .withColumn("LICENSE", F.lit(None).cast("string"))

# Union both datasets
combined_df = chicago_aligned.unionByName(dallas_aligned)

# Drop duplicates
combined_df = combined_df.dropDuplicates(["BUSINESS_NAME", "BUSINESS_TYPE"])

# Add surrogate key (BUSINESS_ID)
window_spec = Window.orderBy("BUSINESS_NAME")
combined_df = combined_df.withColumn("BUSINESS_ID", F.row_number().over(window_spec))

# Add JOB_ID and LOAD_DT
combined_df = combined_df.withColumn("JOB_ID", F.lit("job_001")) \
                                     .withColumn("LOAD_DT", F.current_date())

# Final column selection and order
dim_business_df = combined_df.select(
    "BUSINESS_ID",
    "BUSINESS_NAME",
    "AKA_NAME",
    "BUSINESS_TYPE",
    "LICENSE",
    "JOB_ID",
    "LOAD_DT"
)

# Write to Snowflake
dim_business_df.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "DIM_BUSINESS") \
    .mode("overwrite") \
    .save()

print("✅ DIM_BUSINESS table successfully loaded with fixed JOB_ID = 13!")
