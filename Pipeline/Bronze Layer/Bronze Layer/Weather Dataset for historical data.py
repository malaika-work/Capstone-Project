# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer — Weather Data for Historical Orders
# MAGIC Generates randomized weather codes for historical (original Olist) orders.
# MAGIC Maps codes to human-readable categories for Risk Analysis.
# MAGIC
# MAGIC | Code | Category | Description |
# MAGIC | :--- | :--- | :--- |
# MAGIC | **0** | **Clear Sky** | No clouds or very few clouds. |
# MAGIC | **1, 2, 3** | **Cloudy** | 1 = Mainly clear, 2 = Partly cloudy, 3 = Overcast. |
# MAGIC | **45, 48** | **Fog** | 45 = Fog, 48 = Depositing rime fog. |
# MAGIC | **51, 53, 55** | **Drizzle** | Light / Moderate / Dense. |
# MAGIC | **61, 63, 65** | **Rain** | Slight / Moderate / Heavy. |
# MAGIC | **71, 73, 75** | **Snow** | Slight / Moderate / Heavy. |

# COMMAND ----------

# ─── Load credentials from Secret Scope ───────────────────────────
SCOPE = "secret-scope"

STORAGE_ACCOUNT = "ecomadlsdev"
STORAGE_KEY     = dbutils.secrets.get(scope=SCOPE, key="capstone-secret-storage-scope")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    STORAGE_KEY
)

BASE_PATH = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
print(f"Storage Account: {STORAGE_ACCOUNT}")
print("Credentials loaded from Key Vault")

# COMMAND ----------

from pyspark.sql.functions import col, to_date, rand, when


orders = spark.read.format("delta").load(f"{BASE_PATH}/orders")
cust = spark.read.format("delta").load(f"{BASE_PATH}/customers")
geo = spark.read.format("delta").load(f"{BASE_PATH}/geolocation").dropDuplicates(["geolocation_zip_code_prefix"])

hist_base = orders.join(cust, "customer_id") \
    .join(geo, col("customer_zip_code_prefix") == col("geolocation_zip_code_prefix")) \
    .select(
        col("geolocation_lat").alias("lat"),
        col("geolocation_lng").alias("lon"),
        to_date("order_purchase_timestamp").alias("purchase_date")
    )

hist_weather = hist_base.withColumn("r", rand() * 100) \
    .withColumn("weather_code", 
        when(col("r") < 30, "0")   
        .when(col("r") < 40, "1") 
        .when(col("r") < 45, "2")
        .when(col("r") < 50, "3")
        .when(col("r") < 59, "45")
        .when(col("r") < 60, "48")   
        .when(col("r") < 70, "51")  
        .when(col("r") < 72, "53")   
        .when(col("r") < 75, "55")
        .when(col("r") < 90, "61") 
        .when(col("r") < 96, "63") 
        .when(col("r") < 100, "65")    
        .otherwise("71")           
    ) \
    .withColumn("weather_description", 
        when(col("weather_code") == "0", "Clear Sky")
        .when(col("weather_code").isin("1", "2", "3"), "Cloudy")
        .when(col("weather_code").isin("45", "48"), "Fog")
        .when(col("weather_code").isin("51", "53", "55"), "Drizzle")
        .when(col("weather_code").isin("61", "63", "65"), "Rain")
        .when(col("weather_code").isin("71", "73", "75"), "Snow")
        .otherwise("Unknown")
    ).drop("r")

# COMMAND ----------

output_path = f"{BASE_PATH}/weather"
hist_weather.write.format("delta").mode("overwrite").save(output_path)
print(f"Historical weather data written to {output_path}")

# COMMAND ----------

hist_weather.display()

# COMMAND ----------

print(f"Total rows: {hist_weather.count()}")

# COMMAND ----------

