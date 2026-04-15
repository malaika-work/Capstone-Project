# Databricks notebook source
# MAGIC %md
# MAGIC ## Customrs Table

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

# DBTITLE 1,Load customers table from Bronze layer
# Load customers table from Bronze layer (read-only)
bronze_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/customers"

df = spark.read.format("delta").load(bronze_path)

print(f"Row count: {df.count()}")
print(f"Columns: {df.columns}")
display(df)

# COMMAND ----------

# DBTITLE 1,Print customers table schema
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## checking duplicates 

# COMMAND ----------

# DBTITLE 1,Check for duplicate customer IDs
from pyspark.sql.functions import count

duplicates = (
    df.groupBy("customer_id")
    .agg(count("*").alias("cnt"))
    .filter("cnt > 1")
)

print(f"Duplicate customer_id count: {duplicates.count()}")
display(duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC there are no duplicates in the customer id 

# COMMAND ----------

# MAGIC %md
# MAGIC ## check for the null values 

# COMMAND ----------

# DBTITLE 1,Check null values in selected columns
from pyspark.sql.functions import col, sum as spark_sum

null_counts = df.select(
    spark_sum(col("customer_id").isNull().cast("int")).alias("customer_id"),
    spark_sum(col("customer_unique_id").isNull().cast("int")).alias("customer_unique_id"),
    spark_sum(col("customer_zip_code_prefix").isNull().cast("int")).alias("customer_zip_code_prefix")
)

display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC there are no null values in this columns 

# COMMAND ----------

# DBTITLE 1,Data Standardization
# MAGIC %md
# MAGIC ## Data Standardization

# COMMAND ----------

# DBTITLE 1,Standardize customer data
from pyspark.sql.functions import col, trim, initcap, upper

df_standardized = df.select(
    trim(col("customer_id")).alias("customer_id"),
    trim(col("customer_unique_id")).alias("customer_unique_id"),
    col("customer_zip_code_prefix"),
    initcap(trim(col("customer_city"))).alias("customer_city"),
    upper(trim(col("customer_state"))).alias("customer_state")
)

print("Before standardization:")
df.select("customer_city", "customer_state").show(5, truncate=False)

print("After standardization:")
df_standardized.select("customer_city", "customer_state").show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Metadata Columns
# MAGIC %md
# MAGIC ## Add Metadata Columns

# COMMAND ----------

# DBTITLE 1,Add metadata columns to new table
from pyspark.sql.functions import current_timestamp, lit

# Create a new table with metadata columns — original df_standardized remains unchanged
df_final = df_standardized \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source", lit("bronze_customers")) \
    .withColumn("layer", lit("silver"))

print(f"Original columns: {df_standardized.columns}")
print(f"New columns: {df_final.columns}")
print(f"Row count: {df_final.count()}")
display(df_final)

# COMMAND ----------

# DBTITLE 1,Save customers table to Silver layer
# Save df_final to the Silver layer as a Delta table
# Bronze layer remains untouched as per instructions
silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/customers"

df_final.write.format("delta").mode("overwrite").save(silver_path)

print(f"Customers table saved to: {silver_path}")