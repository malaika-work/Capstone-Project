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

# DBTITLE 1,Load products table from Bronze layer
# Load products table from Bronze layer (read-only as per instructions)
bronze_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/products"

df = spark.read.format("delta").load(bronze_path)

print(f"Row count: {df.count()}")
df.printSchema()
display(df)

# COMMAND ----------

# DBTITLE 1,Identify Bad Records
# MAGIC %md
# MAGIC ## Identify Bad Records

# COMMAND ----------

# DBTITLE 1,Filter and save bad records
from pyspark.sql.functions import col, current_timestamp, lit, lower, trim, when

source_file = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/products"

# Identify bad records where product_category_name is null
bad_records = df.filter(col("product_category_name").isNull())
bad_record_count = bad_records.count()
print(f"Bad records found: {bad_record_count}")

bad_records = (
    bad_records
    .withColumn("error_reason", lit("product_category_name is null"))
    .withColumn("error_timestamp", current_timestamp())
    .withColumn("file_location", lit(source_file))
)

# Save bad records to silver layer
bad_records_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/product_bad_records"
bad_records.write.format("delta").mode("overwrite").save(bad_records_path)

print(f"Bad records saved to: {bad_records_path}")
display(bad_records)

# COMMAND ----------

# DBTITLE 1,Clean Valid Records
# MAGIC %md
# MAGIC ## Clean Valid Records

# COMMAND ----------

# DBTITLE 1,Standardize and prepare silver products
# Clean valid records - original df remains unchanged
silver_product = (
    df
    
    # Remove bad records
    .filter(col("product_category_name").isNotNull())
    
    # Standardization
    .withColumn("product_category_name", lower(trim(col("product_category_name"))))
    
    # Metadata columns
    .withColumn("create_date", current_timestamp())
    .withColumn("update_date", current_timestamp())
    .withColumn("file_location", lit(source_file))
    
    # Final schema
    .select(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "create_date",
        "update_date",
        "file_location"
    )
)

print(f"Original count: {df.count()}")
print(f"Bad records: {bad_record_count}")
print(f"Silver product count: {silver_product.count()}")
display(silver_product)

# COMMAND ----------

# DBTITLE 1,Save products table to Silver layer
# Save silver_product to Azure Silver layer
# Bronze layer remains untouched as per instructions
silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/products"

silver_product.write.format("delta").mode("overwrite").save(silver_path)

print(f"Products table saved to: {silver_path}")