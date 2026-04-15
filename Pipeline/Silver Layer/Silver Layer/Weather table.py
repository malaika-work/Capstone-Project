# Databricks notebook source
# DBTITLE 1,Weather Table
# MAGIC %md
# MAGIC # Weather Table

# COMMAND ----------

# DBTITLE 1,Configure Azure Storage credentials
# Configure Azure Blob Storage credentials
storage_account_name = "capstonestorageaccount03"
storage_account_key = dbutils.secrets.get(scope="capstosne-scope", key="storagekey")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# DBTITLE 1,Load weather table from Bronze layer
# Load weather table from Bronze layer (read-only as per instructions)
bronze_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/weather"

df = spark.read.format("delta").load(bronze_path)

print(f"Row count: {df.count()}")
df.printSchema()
display(df)

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# DBTITLE 1,Check nulls, duplicates and invalid records
from pyspark.sql.functions import col, sum as spark_sum, count

# Null counts
null_counts = df.select(
    [spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
)
print("Null values per column:")
display(null_counts)

# Duplicate check on (lat, lon, purchase_date)
duplicates = df.groupBy("lat", "lon", "purchase_date").agg(count("*").alias("cnt")).filter("cnt > 1")
print(f"\nDuplicate (lat, lon, purchase_date) count: {duplicates.count()}")

# Invalid coordinates (outside Brazil bounds)
invalid_coords = df.filter(
    (col("lat") < -34) | (col("lat") > 6) |
    (col("lon") < -74) | (col("lon") > -35)
).count()
print(f"Records with invalid coordinates: {invalid_coords}")

# Date range check
from pyspark.sql.functions import min as spark_min, max as spark_max
date_range = df.select(spark_min("purchase_date").alias("min_date"), spark_max("purchase_date").alias("max_date"))
print("\nDate range:")
display(date_range)

# COMMAND ----------

# DBTITLE 1,Cleaning and Transformation
# MAGIC %md
# MAGIC ## Cleaning, Type Casting & Standardization

# COMMAND ----------

# DBTITLE 1,Apply all transformations
from pyspark.sql.functions import col, lower, trim, round as spark_round, current_timestamp, lit

source_file = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/weather"

# Original df remains unchanged
df_final = (
    df
    
    # Remove nulls in critical columns
    .filter(
        col("lat").isNotNull() &
        col("lon").isNotNull() &
        col("purchase_date").isNotNull() &
        col("weather_code").isNotNull()
    )
    
    # Remove duplicates on (lat, lon, purchase_date)
    .dropDuplicates(["lat", "lon", "purchase_date"])
    
    # Validate coordinates (Brazil bounds)
    .filter(
        (col("lat").between(-34, 6)) &
        (col("lon").between(-74, -35))
    )
    
    # Cast weather_code from string to integer
    .withColumn("weather_code", col("weather_code").cast("integer"))
    
    # Round coordinates to 4 decimal places for consistent joins
    .withColumn("lat", spark_round(col("lat"), 4))
    .withColumn("lon", spark_round(col("lon"), 4))
    
    # Standardize weather_description
    .withColumn("weather_description", lower(trim(col("weather_description"))))
    
    # Metadata columns
    .withColumn("create_date", current_timestamp())
    .withColumn("update_date", current_timestamp())
    .withColumn("file_location", lit(source_file))
    
    # Final schema
    .select(
        "lat",
        "lon",
        "purchase_date",
        "weather_code",
        "weather_description",
        "create_date",
        "update_date",
        "file_location"
    )
)

print(f"Original count: {df.count()}")
print(f"Cleaned count: {df_final.count()}")
print(f"Rows removed: {df.count() - df_final.count()}")
df_final.printSchema()
display(df_final)

# COMMAND ----------

# DBTITLE 1,Save to Silver Layer
# MAGIC %md
# MAGIC ## Save to Silver Layer

# COMMAND ----------

# DBTITLE 1,Save weather table to Silver layer
# Save df_final to the Silver layer as a Delta table
# Bronze layer remains untouched as per instructions
silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/weather"

df_final.write.format("delta").mode("overwrite").save(silver_path)

print(f"Weather table saved to: {silver_path}")