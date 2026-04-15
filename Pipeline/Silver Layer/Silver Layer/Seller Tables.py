# Databricks notebook source
# DBTITLE 1,Configure Azure Storage credentials
# Configure Azure Blob Storage credentials
storage_account_name = "capstonestorageaccount03"
storage_account_key = dbutils.secrets.get(scope="capstosne-scope", key="storagekey")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# DBTITLE 1,Load sellers table from Bronze layer
# Load sellers table from Bronze layer (read-only as per instructions)
bronze_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/sellers"

df = spark.read.format("delta").load(bronze_path)

print(f"Row count: {df.count()}")
df.printSchema()
display(df)

# COMMAND ----------

# DBTITLE 1,Data Quality Checks
# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# DBTITLE 1,Check nulls and duplicates
from pyspark.sql.functions import col, sum as spark_sum, count

# Null counts
null_counts = df.select(
    [spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
)
print("Null values per column:")
display(null_counts)

# Duplicate seller_id check
duplicates = df.groupBy("seller_id").agg(count("*").alias("cnt")).filter("cnt > 1")
print(f"\nDuplicate seller_id count: {duplicates.count()}")

# COMMAND ----------

# DBTITLE 1,Data Cleaning and Standardization
# MAGIC %md
# MAGIC ## Data Cleaning & Standardization

# COMMAND ----------

# DBTITLE 1,Clean, standardize and add metadata
from pyspark.sql.functions import col, initcap, upper, lower, trim, current_timestamp, lit, regexp_replace

source_file = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/sellers"

# Original df remains unchanged
df_final = (
    df
    
    # Remove null primary key
    .filter(col("seller_id").isNotNull())
    
    # Remove duplicates
    .dropDuplicates(["seller_id"])
    
    # Fix dirty city names (e.g., "lages - sc" -> "lages")
    .withColumn("seller_city", regexp_replace(trim(col("seller_city")), r"\s*-\s*\w+$", ""))
    
    # Standardization
    .withColumn("seller_city", initcap(trim(col("seller_city"))))
    .withColumn("seller_state", upper(trim(col("seller_state"))))
    
    # Metadata columns
    .withColumn("create_date", current_timestamp())
    .withColumn("update_date", current_timestamp())
    .withColumn("file_location", lit(source_file))
    
    # Final schema
    .select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "create_date",
        "update_date",
        "file_location"
    )
)

print(f"Original count: {df.count()}")
print(f"Cleaned count: {df_final.count()}")
print(f"Rows removed: {df.count() - df_final.count()}")
display(df_final)

# COMMAND ----------

# DBTITLE 1,Save sellers table to Silver layer
# Save df_final to the Silver layer as a Delta table
# Bronze layer remains untouched as per instructions
silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/sellers"

df_final.write.format("delta").mode("overwrite").save(silver_path)

print(f"Sellers table saved to: {silver_path}")