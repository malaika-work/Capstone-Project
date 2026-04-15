# Databricks notebook source
# MAGIC %md
# MAGIC # Orders Table

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. loading tables from azure blobs

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("capstosne-scope")

# COMMAND ----------

# DBTITLE 1,Configure Azure Storage credentials

storage_account_name = "capstonestorageaccount03"
storage_account_key = dbutils.secrets.get(scope="capstosne-scope", key="storagekey")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# COMMAND ----------

bronze_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/orders"

df = spark.read.format("delta").load(bronze_path)

# COMMAND ----------

display(df)
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## NUll Values

# COMMAND ----------

# DBTITLE 1,Count null values per column
from pyspark.sql.functions import col, sum as spark_sum

null_counts = df.select(
    [spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
)

display(null_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC Null values appear in those columns because not all orders complete every stage (approval, shipping, delivery), so missing timestamps reflect real business states like cancelled or pending orders.

# COMMAND ----------

# MAGIC %md
# MAGIC so we are not removing these null values because they can be in helped in future analysis
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## checking for Duplicates order_id

# COMMAND ----------

# DBTITLE 1,Check for duplicate order_ids
from pyspark.sql.functions import count

duplicates = (
    df.groupBy("order_id")
    .agg(count("*").alias("cnt"))
    .filter("cnt > 1")
)

print(f"Duplicate order_id count: {duplicates.count()}")
display(duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC therefore there are no duplicates order id 

# COMMAND ----------

# MAGIC %md
# MAGIC ## validating dates

# COMMAND ----------

# DBTITLE 1,Check for invalid date sequences
from pyspark.sql.functions import col

# Check for logical date inconsistencies
invalid_dates = df.filter(
    (col("order_approved_at") < col("order_purchase_timestamp")) |
    (col("order_delivered_carrier_date") < col("order_purchase_timestamp")) |
    (col("order_delivered_customer_date") < col("order_purchase_timestamp")) |
    (col("order_delivered_customer_date") < col("order_delivered_carrier_date"))
)

print(f"Total rows with invalid date sequences: {invalid_dates.count()}")
display(invalid_dates)

# COMMAND ----------

# DBTITLE 1,Create cleaned table excluding invalid dates
# Create a cleaned DataFrame excluding the 189 invalid date records
# Original df remains unchanged for future use

df_cleaned = df.join(invalid_dates, on="order_id", how="left_anti")

print(f"Original table count: {df.count()}")
print(f"Cleaned table count: {df_cleaned.count()}")
print(f"Records removed: {df.count() - df_cleaned.count()}")

display(df_cleaned)

# COMMAND ----------

# DBTITLE 1,Save cleaned table to Silver layer
# Save the cleaned DataFrame to the Silver layer as a Delta table
silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/orders"

df_cleaned.write.format("delta").mode("overwrite").save(silver_path)

print(f"Cleaned orders table saved to: {silver_path}")

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM delta.`wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/orders` RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## final silver orders

# COMMAND ----------

silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/orders"

df_silver = spark.read.format("delta").load(silver_path)

print(f"Row count: {df_silver.count()}")
print(f"Columns: {df_silver.columns}")
display(df_silver)