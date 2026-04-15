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

# DBTITLE 1,Load category translation table from Bronze layer
# Load category translation table from Bronze layer (read-only as per instructions)
bronze_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/category_translation"

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

# Duplicate check
duplicates = df.groupBy("product_category_name").agg(count("*").alias("cnt")).filter("cnt > 1")
print(f"\nDuplicate product_category_name count: {duplicates.count()}")

# COMMAND ----------

# DBTITLE 1,Clean, Standardize and Save
# MAGIC %md
# MAGIC ## Clean, Standardize & Save

# COMMAND ----------

# DBTITLE 1,Clean, standardize and add metadata
from pyspark.sql.functions import col, lower, trim, current_timestamp, lit

source_file = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/category_translation"

# Original df remains unchanged
df_final = (
    df
    
    # Remove null values in either column
    .filter(col("product_category_name").isNotNull() & col("product_category_name_english").isNotNull())
    
    # Remove duplicates
    .dropDuplicates(["product_category_name"])
    
    # Standardize to lowercase + trim
    .withColumn("product_category_name", lower(trim(col("product_category_name"))))
    .withColumn("product_category_name_english", lower(trim(col("product_category_name_english"))))
    
    # Metadata columns
    .withColumn("create_date", current_timestamp())
    .withColumn("update_date", current_timestamp())
    .withColumn("file_location", lit(source_file))
)

print(f"Original count: {df.count()}")
print(f"Cleaned count: {df_final.count()}")
print(f"Rows removed: {df.count() - df_final.count()}")
display(df_final)

# COMMAND ----------

# DBTITLE 1,Save to Silver layer
# Save to Silver layer - bronze remains untouched as per instructions
silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/category_translation"

df_final.write.format("delta").mode("overwrite").save(silver_path)

print(f"Category translation table saved to: {silver_path}")