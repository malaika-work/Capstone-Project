# Databricks notebook source
# DBTITLE 1,Load geolocation and print schema
# Configure Azure Blob Storage credentials
storage_account_name = "capstonestorageaccount03"
storage_account_key = dbutils.secrets.get(scope="capstosne-scope", key="storagekey")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

# COMMAND ----------

# DBTITLE 1,Load geolocation and print schema
# Load geolocation table from Bronze layer (read-only as per instructions)
bronze_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/geolocation"

df_geolocation = spark.read.format("delta").load(bronze_path)

# Print schema
df_geolocation.printSchema()
print(f"Row count: {df_geolocation.count()}")
display(df_geolocation)

# COMMAND ----------

# DBTITLE 1,Clean and Deduplicate Geolocation Records
# MAGIC %md
# MAGIC ## Clean and Deduplicate Geolocation Records

# COMMAND ----------

# DBTITLE 1,Standardize and deduplicate silver geolocation
from pyspark.sql.functions import col, upper, trim, avg, first, current_timestamp, lit

source_file = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/bronze/geolocation"

silver_geo = (
    df_geolocation
    # Standardization
    .withColumn("geolocation_city", upper(trim(col("geolocation_city"))))
    .withColumn("geolocation_state", upper(trim(col("geolocation_state"))))
    .withColumn("source_file", lit(source_file))
    # Deduplicate by zip code and aggregate
    .groupBy(
        "geolocation_zip_code_prefix",
        "geolocation_city",
        "geolocation_state"
    )
    .agg(
        avg("geolocation_lat").alias("geolocation_lat"),
        avg("geolocation_lng").alias("geolocation_lng"),
        first("source_file").alias("file_location")
    )
    # Metadata
    .withColumn("create_date", current_timestamp())
    .withColumn("update_date", current_timestamp())
    # Final schema
    .select(
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
        "create_date",
        "update_date",
        "file_location"
    )
)

print(f"Original count: {df_geolocation.count()}")
print(f"Silver geolocation count: {silver_geo.count()}")
display(silver_geo)

# COMMAND ----------

# DBTITLE 1,Save Silver Geolocation
# MAGIC %md
# MAGIC ## Save Silver Geolocation

# COMMAND ----------

# DBTITLE 1,Save geolocation to Silver layer
# Save silver_geo to Azure Silver layer
# As per instructions, Bronze layer remains untouched
silver_path = "wasbs://capstone-project-pks@capstonestorageaccount03.blob.core.windows.net/silver/geolocation"

silver_geo.write.format("delta").mode("overwrite").save(silver_path)

print(f"Geolocation table saved to: {silver_path}")