# Databricks notebook source
# MAGIC %md
# MAGIC # Reviews Table 

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

# DBTITLE 1,Load reviews table from Bronze layer
# Load reviews table from Bronze layer (read-only as per instructions)
bronze_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/reviews"

df = spark.read.format("delta").load(bronze_path)

print(f"Row count: {df.count()}")
df.printSchema()
display(df)

# COMMAND ----------

# DBTITLE 1,Clean, standardize and add metadata
from pyspark.sql.functions import col, lower, trim, current_timestamp, lit

source_file = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/reviews"

# Original df remains unchanged
df_final = (
    df
    # Remove null primary key
    .filter(col("review_id").isNotNull())
    
    # Remove duplicates
    .dropDuplicates(["review_id"])
    
    # Remove null order_id
    .filter(col("order_id").isNotNull())
    
    # Standardization
    .withColumn("review_comment_title", lower(trim(col("review_comment_title"))))
    .withColumn("review_comment_message", lower(trim(col("review_comment_message"))))
    
    # Metadata columns
    .withColumn("create_date", current_timestamp())
    .withColumn("update_date", current_timestamp())
    .withColumn("file_location", lit(source_file))
    
    # Select final columns
    .select(
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
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

# DBTITLE 1,Save reviews table to Silver layer
# Save df_final to the Silver layer as a Delta table
# Bronze layer remains untouched as per instructions
silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/reviews"

df_final.write.format("delta").mode("overwrite").save(silver_path)

print(f"Reviews table saved to: {silver_path}")

# COMMAND ----------

display(df_final)