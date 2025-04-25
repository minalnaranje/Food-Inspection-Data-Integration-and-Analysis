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
    "sfRole": "FOODINSP_BA",
}

# Load staging data
stg_chicago_df = spark.read.format("snowflake").options(**sfOptions).option("dbtable", "STG_CHICAGO").load()
stg_dallas_df = spark.read.format("snowflake").options(**sfOptions).option("dbtable", "STG_DALLAS").load()

# Combine both datasets for FACT
combined_df = stg_chicago_df.select(
    "INSPECTION_DATE", "FACILITY_TYPE", "RISK_COUNT", "VIOLATIONS_NEW", "INSPECTION_TYPE",
    F.col("DBA_NAME").alias("BUSINESS_NAME"),
    F.col("INSPECTION_SCORE"), F.col("VIOLATIONS_COUNT"),
    "STREET_NUMBER", "STREET_DIRECTION", "STREET_NAME", "STREET_TYPE", "CITY",
    F.col("ZIP").alias("ZIP_CODE"), "LATITUDE", "LONGITUDE"
).unionByName(
    stg_dallas_df.select(
        "INSPECTION_DATE", "FACILITY_TYPE", 
        F.lit(None).cast("int").alias("RISK_COUNT"),  # Dallas doesn't have RISK_COUNT
        F.col("VIOLATION_DESCRIPTION").alias("VIOLATIONS_NEW"), "INSPECTION_TYPE",
        F.col("RESTAURANT_NAME").alias("BUSINESS_NAME"),
        F.col("INSPECTION_SCORE"), F.col("VIOLATION_COUNT").alias("VIOLATIONS_COUNT"),
        "STREET_NUMBER", "STREET_DIRECTION", "STREET_NAME", "STREET_TYPE", "CITY",
        "ZIP_CODE", "LATITUDE", "LONGITUDE"
    )
)

# Load DIM tables
dim_business = spark.read.format("snowflake").options(**sfOptions).option("dbtable", "DIM_BUSINESS").load()
dim_location = spark.read.format("snowflake").options(**sfOptions).option("dbtable", "DIM_LOCATION").load()
dim_date = spark.read.format("snowflake").options(**sfOptions).option("dbtable", "DIM_DATE").load()
dim_violation = spark.read.format("snowflake").options(**sfOptions).option("dbtable", "DIM_VIOLATION").load()
dim_inspection = spark.read.format("snowflake").options(**sfOptions).option("dbtable", "DIM_INSPECTION").load()

# Join with DIM tables
fact_df = combined_df.join(dim_business, on="BUSINESS_NAME", how="left") \
    .join(dim_location, on=["STREET_NUMBER", "STREET_DIRECTION", "STREET_NAME", "STREET_TYPE", "CITY", "ZIP_CODE", "LATITUDE", "LONGITUDE"], how="left") \
    .join(dim_date, on="INSPECTION_DATE", how="left") \
    .join(dim_violation, combined_df["VIOLATIONS_NEW"] == dim_violation["VIOLATION_DESCRIPTION"], how="left") \
    .join(dim_inspection, on="INSPECTION_TYPE", how="left")

# ✅ Remove duplicate fact rows before surrogate key generation
fact_df_deduped = fact_df.dropDuplicates([
    "BUSINESS_ID", "LOCATION_ID_SK", "DATE_SK", 
    "VIOLATION_ID_SK", "INSPECTION_TYPE_SK", "INSPECTION_SCORE"
])

# Add surrogate key for FACT table
window_spec = Window.orderBy("INSPECTION_DATE", "BUSINESS_NAME")
fact_df_deduped = fact_df_deduped.withColumn("INSPECTION_ID_SK", F.row_number().over(window_spec))

# Drop earlier JOB_ID and LOAD_DT if present, then add new ones
fact_df_deduped = fact_df_deduped.drop("JOB_ID", "LOAD_DT", "VIOLATIONS_NEW")
fact_df_deduped = fact_df_deduped.withColumn("JOB_ID", F.lit(13)) \
                                 .withColumn("LOAD_DT", F.current_date())

# Final selection for FACT_INSPECTION
fact_final = fact_df_deduped.select(
    "INSPECTION_ID_SK",
    "BUSINESS_ID",
    "LOCATION_ID_SK",
    "DATE_SK",
    "VIOLATION_ID_SK",
    "INSPECTION_TYPE_SK",
    "INSPECTION_SCORE",
    "VIOLATIONS_COUNT",
    F.col("RISK_COUNT").alias("RISK_SCORE"),
    "JOB_ID",
    "LOAD_DT"
)

# Write to Snowflake
fact_final.write \
    .format("snowflake") \
    .options(**sfOptions) \
    .option("dbtable", "FACT_INSPECTION") \
    .mode("overwrite") \
    .save()

print("✅ FACT_INSPECTION table de-duplicated and successfully loaded into Snowflake!")