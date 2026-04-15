# Databricks notebook source
# MAGIC %md
# MAGIC # Payments Table

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

# DBTITLE 1,Load payments table from Bronze layer
# Load payments table from Bronze layer (read-only as per instructions)
bronze_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/payments"

df = spark.read.format("delta").load(bronze_path)

print(f"Row count: {df.count()}")
print(f"Columns: {df.columns}")
display(df)

# COMMAND ----------

# DBTITLE 1,Print payments table schema
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## NUll Values 

# COMMAND ----------

# DBTITLE 1,Remove null order_id and payment_sequential
from pyspark.sql.functions import col

# Remove rows where order_id or payment_sequential is null
# Original df remains unchanged
df_cleaned = df.filter(
    col("order_id").isNotNull() &
    col("payment_sequential").isNotNull()
)

print(f"Original count: {df.count()}")
print(f"Cleaned count: {df_cleaned.count()}")
print(f"Rows removed: {df.count() - df_cleaned.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Duplicates 

# COMMAND ----------

# DBTITLE 1,Check for duplicate order_id + payment_sequential
from pyspark.sql.functions import count

duplicates = (
    df_cleaned.groupBy("order_id", "payment_sequential")
    .agg(count("*").alias("cnt"))
    .filter("cnt > 1")
)

print(f"Duplicate (order_id, payment_sequential) count: {duplicates.count()}")
display(duplicates)

# COMMAND ----------

# DBTITLE 1,Validate Numeric
# MAGIC %md
# MAGIC ## Validate Numeric Values

# COMMAND ----------

# DBTITLE 1,Validate payment_value and payment_installments >= 0
from pyspark.sql.functions import col

invalid_records = df_cleaned.filter(
    ~((col("payment_value") >= 0) &
      (col("payment_installments") >= 0))
)

print(f"Rows with payment_value < 0: {df_cleaned.filter(col('payment_value') < 0).count()}")
print(f"Rows with payment_installments < 0: {df_cleaned.filter(col('payment_installments') < 0).count()}")
print(f"Total invalid rows: {invalid_records.count()}")
display(invalid_records)

# COMMAND ----------

# DBTITLE 1,Standardization and Metadata
# MAGIC %md
# MAGIC ## Standardization & Metadata Columns

# COMMAND ----------

# DBTITLE 1,Standardize and add metadata columns
from pyspark.sql.functions import col, upper, trim, current_timestamp, lit

source_file = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/payments"

# Original df_cleaned remains unchanged
df_final = (
    df_cleaned
    # Standardization
    .withColumn("payment_type", upper(trim(col("payment_type"))))
    
    # Metadata columns
    .withColumn("create_date", current_timestamp())
    .withColumn("update_date", current_timestamp())
    .withColumn("file_location", lit(source_file))
    
    # Final schema
    .select(
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
        "create_date",
        "update_date",
        "file_location"
    )
)

print(f"Row count: {df_final.count()}")
print(f"Columns: {df_final.columns}")
display(df_final)

# COMMAND ----------

# DBTITLE 1,Save payments table to Silver layer
# Save df_final to the Silver layer as a Delta table
# Bronze layer remains untouched as per instructions
silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/payments"

df_final.write.format("delta").mode("overwrite").save(silver_path)

print(f"Payments table saved to: {silver_path}")