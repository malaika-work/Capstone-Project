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

# DBTITLE 1,Load order_items from Bronze layer
# Load order_items table from Bronze layer (read-only as per instructions)
bronze_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/order_items"

df = spark.read.format("delta").load(bronze_path)

print(f"Row count: {df.count()}")
print(f"Columns: {df.columns}")
display(df)

# COMMAND ----------

# DBTITLE 1,Print order_items schema
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## null Values 

# COMMAND ----------

# DBTITLE 1,Count null values per column
from pyspark.sql.functions import col, sum as spark_sum

null_counts = df.select(
    [spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
)

display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## duplicate reocrds

# COMMAND ----------

# DBTITLE 1,Check for duplicate order_id + order_item_id
from pyspark.sql.functions import count

duplicates = (
    df.groupBy("order_id", "order_item_id")
    .agg(count("*").alias("cnt"))
    .filter("cnt > 1")
)

print(f"Duplicate (order_id, order_item_id) count: {duplicates.count()}")
display(duplicates)

# COMMAND ----------

# DBTITLE 1,Data Validation
# MAGIC %md
# MAGIC ## Data Validation

# COMMAND ----------

# DBTITLE 1,Validate price and freight_value >= 0
from pyspark.sql.functions import col

invalid_price = df.filter(col("price") < 0).count()
invalid_freight = df.filter(col("freight_value") < 0).count()

print(f"Rows with price < 0: {invalid_price}")
print(f"Rows with freight_value < 0: {invalid_freight}")

# COMMAND ----------

# DBTITLE 1,Add metadata and save to Silver layer
from pyspark.sql.functions import current_timestamp, lit

# Add metadata columns — original df remains unchanged
df_final = df \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source", lit("bronze_order_items")) \
    .withColumn("layer", lit("silver"))

# Save to Silver layer (bronze remains untouched as per instructions)
silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/order_items"

df_final.write.format("delta").mode("overwrite").save(silver_path)

print(f"Order items table saved to: {silver_path}")
print(f"Row count: {df_final.count()}")
print(f"Columns: {df_final.columns}")