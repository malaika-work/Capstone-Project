# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer — Weather Data for Synthetic Sales Orders
# MAGIC
# MAGIC Calls the **Open-Meteo Archive API** to get real weather codes for
# MAGIC synthetic (generated) orders, then writes weather data to bronze Delta.
# MAGIC
# MAGIC | Code | General Category | Description |
# MAGIC | :--- | :--- | :--- |
# MAGIC | **0** | **Clear Sky** | No clouds or very few clouds. |
# MAGIC | **1, 2, 3** | **Cloudy** | 1 = Mainly clear, 2 = Partly cloudy, 3 = Overcast. |
# MAGIC | **45, 48** | **Fog** | 45 = Fog, 48 = Depositing rime fog (freezing fog). |
# MAGIC | **51, 53, 55** | **Drizzle** | 51 = Light, 53 = Moderate, 55 = Dense intensity. |
# MAGIC | **61, 63, 65** | **Rain** | 61 = Slight, 63 = Moderate, 65 = Heavy intensity. |
# MAGIC | **71, 73, 75** | **Snow** | 71 = Slight, 73 = Moderate, 75 = Heavy intensity. |

# COMMAND ----------

# ─── Load credentials from Secret Scope ───────────────────────────
SCOPE = "secret-scope"

STORAGE_ACCOUNT = "ecomadlsdev"
STORAGE_KEY     = dbutils.secrets.get(scope=SCOPE, key="capstone-secret-storage-scope")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

BASE_PATH    = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
WEATHER_PATH = f"{BASE_PATH}/weather"

print(f"Storage Account: {STORAGE_ACCOUNT}")
print(f"Weather path   : {WEATHER_PATH}")
print("Credentials loaded from Key Vault ✓")

# COMMAND ----------

import requests
from pyspark.sql.functions import udf, col, to_date, when
from pyspark.sql.types import StringType

def get_open_meteo_history(lat, lon, purchase_date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    date_only = str(purchase_date).split(" ")[0] 
    params = {
        "latitude": lat, "longitude": lon,
        "start_date": date_only, "end_date": date_only,
        "daily": "weather_code", "timezone": "America/Sao_Paulo"
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            code = response.json()['daily']['weather_code'][0]
            return str(code) 
        return "API_ERROR"
    except Exception:
        return "NETWORK_ERROR"

weather_udf = udf(get_open_meteo_history, StringType())

# COMMAND ----------

orders = spark.read.format("delta").load(f"{BASE_PATH}/orders")
cust   = spark.read.format("delta").load(f"{BASE_PATH}/customers")
geo    = spark.read.format("delta").load(f"{BASE_PATH}/geolocation")

# COMMAND ----------

try:
    existing_weather = spark.read.format("delta").load(WEATHER_PATH)
    has_existing_data = True
except Exception:
    has_existing_data = False

# COMMAND ----------

# 1. Start with synthetic orders (order_id starting with "g")
new_orders_potential = orders.filter(col("order_id").like("g%"))
initial_count = new_orders_potential.count()

# 2. Deduplicate geo data to prevent row explosion
geo_unique = geo.dropDuplicates(["geolocation_zip_code_prefix"])

# 3. Enrich with location
enriched_new_orders = new_orders_potential \
    .join(cust, "customer_id") \
    .join(geo_unique, col("customer_zip_code_prefix") == col("geolocation_zip_code_prefix")) \
    .select(
        "order_id", 
        col("geolocation_lat").alias("lat"), 
        col("geolocation_lng").alias("lon"),
        to_date("order_purchase_timestamp").alias("purchase_date")
    )

# 4. Filter for truly new records
if has_existing_data:
    orders_to_process = enriched_new_orders.join(
        existing_weather, 
        on=["lat", "lon", "purchase_date"], 
        how="left_anti"
    )
else:
    orders_to_process = enriched_new_orders

# 5. Final count
record_count = orders_to_process.count()

print(f"--- Processing Summary ---")
print(f"Total synthetic orders found: {initial_count}")
print(f"New weather records to fetch: {record_count}")
print(f"Skipped (already in Delta):  {initial_count - record_count if has_existing_data else 0}")

# COMMAND ----------

if record_count > 0:
    weather_results = orders_to_process.withColumn(
        "weather_code", 
        weather_udf(col("lat"), col("lon"), col("purchase_date"))
    )

    weather_final = weather_results.withColumn("weather_description", 
        when(col("weather_code") == "0", "Clear Sky")
        .when(col("weather_code").isin("1", "2", "3"), "Cloudy")
        .when(col("weather_code").isin("45", "48"), "Fog")
        .when(col("weather_code").isin("51", "53", "55"), "Drizzle")
        .when(col("weather_code").isin("61", "63", "65"), "Rain")
        .otherwise("Cloudy/Overcast")
    )

    weather_final = weather_final.drop("order_id")

    weather_final.write.format("delta").mode("append").save(WEATHER_PATH)
    print(f"Successfully processed {record_count} new records")
    
else:
    print("No new data to fetch. Skipping API calls.")

# COMMAND ----------

