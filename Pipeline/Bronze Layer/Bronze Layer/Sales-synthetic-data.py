# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer — Real-Time Sales Ingestion via Azure Event Hub
# MAGIC
# MAGIC Reads streaming synthetic sales data from **your** Azure Event Hub
# MAGIC (`ecom-dev-ehns` / `weather-stream`) using **Databricks Secret Scope**
# MAGIC backed by Azure Key Vault. No hardcoded credentials.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC  Producer → EventHub (weather-stream) → Databricks Structured Streaming → Bronze Delta Tables (ADLS)
# MAGIC ```

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────
# SECTION 1: Load All Credentials from Databricks Secret Scope
# ─────────────────────────────────────────────────────────────────
# Secret Scope "kv-ecom-scope" is backed by your Azure Key Vault.
# Keys are stored in Key Vault manually (as you requested).
#
# Key Vault secret names expected:
#   - "adls-storage-account-name"  → e.g. "ecomadlsdev"
#   - "adls-sas-token"             → SAS token for your new ADLS account
#   - "eventhub-connection-string" → primary connection string of the
#                                    "adf-listener" auth rule on weather-stream hub
#   - "eventhub-namespace"         → e.g. "ecom-dev-ehns"
#   - "eventhub-name"              → "weather-stream"
#
# Run these once in Databricks CLI to create the scope:
#   databricks secrets create-scope kv-ecom-scope \
#     --scope-backend-type AZURE_KEYVAULT \
#     --resource-id /subscriptions/<SUB_ID>/resourceGroups/ecom-dev-rg/providers/Microsoft.KeyVault/vaults/ecom-dev-kv \
#     --dns-name https://ecom-dev-kv.vault.azure.net/

SCOPE = "secret-scope"  # <-- your Databricks secret scope name

STORAGE_ACCOUNT = "ecomadlsdev"
STORAGE_KEY     = dbutils.secrets.get(scope=SCOPE, key="capstone-secret-storage-scope")
EH_CONNECTION   = dbutils.secrets.get(scope=SCOPE, key="eventhub-connection")
EH_NAMESPACE    = dbutils.secrets.get(scope=SCOPE, key="eventhub-namespace")
EH_NAME         = dbutils.secrets.get(scope=SCOPE, key="eventhub-name")

CONTAINER   = "bronze"   # container in your ADLS Gen2 created by Terraform
BRONZE_BASE = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
CHECKPOINT  = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/checkpoints/sales_stream_v1"

# Authenticate to ADLS using Storage Account Key (never expires)
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

print(f"Storage account : {STORAGE_ACCOUNT}")
print(f"EventHub namespace: {EH_NAMESPACE}")
print(f"EventHub name   : {EH_NAME}")
print("Credentials loaded from Key Vault via Secret Scope ✓")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────
# SECTION 2: Configure Kafka Options for Azure Event Hub
# ─────────────────────────────────────────────────────────────────
# Azure Event Hub exposes a Kafka-compatible endpoint on port 9093.
# The "username" is always the literal string "$ConnectionString".
# The "password" is the full connection string from your auth rule.

kafka_options = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "startingOffsets": "earliest",                     # change to "latest" in prod
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        'required username="$ConnectionString" '
        f'password="{EH_CONNECTION}";'
    ),
    # Use the dedicated "databricks-consumer" consumer group defined in your Terraform
    "kafka.group.id": "databricks-consumer",
}

print(f"Connecting to: {EH_NAMESPACE}.servicebus.windows.net:9093")
print(f"Topic        : {EH_NAME}")
print(f"Consumer grp : databricks-consumer")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────
# SECTION 3: Read Raw Stream from Event Hub
# ─────────────────────────────────────────────────────────────────

raw_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# Event Hub messages arrive as binary — cast "value" to string (JSON body)
streaming_df = raw_df.selectExpr("CAST(value AS STRING) as body", "timestamp")

print("Stream source connected ✓")

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────
# SECTION 4: Define Schema & Parse JSON Payload
# ─────────────────────────────────────────────────────────────────

from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

stream_schema = StructType([
    StructField("table",                        StringType()),
    StructField("order_id",                     StringType()),
    StructField("order_item_id",                IntegerType()),
    StructField("product_id",                   StringType()),
    StructField("seller_id",                    StringType()),
    StructField("shipping_limit_date",          StringType()),
    StructField("price",                        DoubleType()),
    StructField("freight_value",                DoubleType()),
    StructField("customer_id",                  StringType()),
    StructField("customer_unique_id",           StringType()),
    StructField("customer_zip_code_prefix",     IntegerType()),
    StructField("customer_city",                StringType()),
    StructField("customer_state",               StringType()),
    StructField("order_status",                 StringType()),
    StructField("order_purchase_timestamp",     StringType()),
    StructField("order_estimated_delivery_date",StringType()),
    StructField("payment_sequential",           IntegerType()),
    StructField("payment_type",                 StringType()),
    StructField("payment_installments",         IntegerType()),
    StructField("payment_value",                DoubleType()),
])

final_streaming_df = (
    streaming_df
    .withColumn("jsonData", from_json(col("body"), stream_schema))
    .select("jsonData.*")
    .withColumn("order_purchase_timestamp",
                to_timestamp(col("order_purchase_timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("order_estimated_delivery_date",
                to_timestamp(col("order_estimated_delivery_date"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("shipping_limit_date",
                to_timestamp(col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss"))
)

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────
# SECTION 5: foreachBatch — Split & Write to Bronze Delta Tables
# ─────────────────────────────────────────────────────────────────
# Each batch fans out to the appropriate Delta table based on
# the "table" field in the JSON payload.

def ingest_to_bronze_tables(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: Empty — skipping.")
        return

    batch_df.persist()

    # Maps the JSON "table" field → (delta folder, columns to keep)
    mapping = {
        "olist_orders_dataset": (
            "orders",
            ["order_id", "customer_id", "order_status",
             "order_purchase_timestamp", "order_estimated_delivery_date"]
        ),
        "olist_order_items_dataset": (
            "order_items",
            ["order_id", "order_item_id", "product_id",
             "seller_id", "shipping_limit_date", "price", "freight_value"]
        ),
        "olist_order_payments_dataset": (
            "payments",
            ["order_id", "payment_sequential", "payment_type",
             "payment_installments", "payment_value"]
        ),
        "olist_customers_dataset": (
            "customers",
            ["customer_id", "customer_unique_id", "customer_zip_code_prefix",
             "customer_city", "customer_state"]
        ),
    }

    for json_key, (folder_name, column_list) in mapping.items():
        table_batch = batch_df.filter(col("table") == json_key).select(*column_list)

        if not table_batch.isEmpty():
            output_path = f"{BRONZE_BASE}/{folder_name}"
            row_count   = table_batch.count()
            print(f"Batch {batch_id}: Writing {row_count} rows → {output_path}")

            (table_batch.write
             .format("delta")
             .option("mergeSchema", "true")
             .mode("append")
             .save(output_path))

    batch_df.unpersist()

# COMMAND ----------

# ─────────────────────────────────────────────────────────────────
# SECTION 6: Start Streaming Query
# ─────────────────────────────────────────────────────────────────
# trigger(availableNow=True) → processes all pending messages then stops.
# Switch to trigger(processingTime="1 minute") for continuous streaming.

query = (
    final_streaming_df.writeStream
    .foreachBatch(ingest_to_bronze_tables)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)   # <-- change to processingTime for live stream
    .start()
)

query.awaitTermination()
print("Bronze ingestion complete")
