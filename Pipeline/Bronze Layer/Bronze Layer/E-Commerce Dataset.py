# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer — E-Commerce Dataset Ingestion
# MAGIC Reads raw CSV files from ADLS `landing/` zone and writes them as Delta tables to `bronze/`.
# MAGIC All credentials loaded from Key Vault via Databricks Secret Scope.

# COMMAND ----------

# ─── Load credentials from Secret Scope ───────────────────────────
SCOPE = "secret-scope"

STORAGE_ACCOUNT = "ecomadlsdev"
STORAGE_KEY     = dbutils.secrets.get(scope=SCOPE, key="capstone-secret-storage-scope")

# Authenticate using Storage Account Key (never expires, unlike SAS tokens)
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

# Use abfss:// (ADLS Gen2 native) — your Terraform created these containers
BASE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
LANDING_PATH = f"abfss://archive@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# LANDING_PATH = f"abfss://landing@{STORAGE_ACCOUNT}.dfs.core.windows.net"

print(f"Storage Account: {STORAGE_ACCOUNT}")
print(f"Landing path   : {LANDING_PATH}")
print(f"Bronze path    : {BASE_PATH}")
print("Credentials loaded from Key Vault")

# COMMAND ----------

datasets = {
    "olist_orders_dataset.csv": "orders",
    "olist_customers_dataset.csv": "customers",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "payments",
    "olist_order_reviews_dataset.csv": "reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "category_translation"
}

# COMMAND ----------

for file_name, table_folder in datasets.items():
    input_path = f"{LANDING_PATH}/{file_name}"
    output_path = f"{BASE_PATH}/{table_folder}"
    
    print(f"Processing {file_name} -> {output_path}")
    
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(input_path)
    
    df.write.format("delta") \
        .mode("overwrite") \
        .save(output_path)

print("E-Commerce dataset ingestion complete ✓")

# COMMAND ----------

