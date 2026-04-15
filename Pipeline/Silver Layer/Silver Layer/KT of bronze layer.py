# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Tables (10)
# MAGIC
# MAGIC | Category                     | Table Name            | Source Type              |
# MAGIC |-----------------------------|----------------------|--------------------------|
# MAGIC | Core Business (E-commerce)  | orders               | CSV + Streaming          |
# MAGIC | Core Business (E-commerce)  | customers            | CSV + Streaming          |
# MAGIC | Core Business (E-commerce)  | order_items          | CSV + Streaming          |
# MAGIC | Core Business (E-commerce)  | payments             | CSV + Streaming          |
# MAGIC | Core Business (E-commerce)  | reviews              | CSV                      |
# MAGIC | Core Business (E-commerce)  | products             | CSV                      |
# MAGIC | Core Business (E-commerce)  | sellers              | CSV                      |
# MAGIC | Core Business (E-commerce)  | geolocation          | CSV                      |
# MAGIC | Core Business (E-commerce)  | category_translation | CSV                      |
# MAGIC | Weather (Special Case)      | weather              | Synthetic + API-based    |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Logic
# MAGIC
# MAGIC - **Batch** → CSV → Delta (overwrite)  
# MAGIC - **Streaming** → Event Hub → micro-batches → Delta (append)  
# MAGIC - **Weather** → Synthetic + API (incremental)  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##  Ingestion Behavior
# MAGIC
# MAGIC | Type | Mode |
# MAGIC |------|------|
# MAGIC | Batch | Overwrite |
# MAGIC | Streaming | Append |
# MAGIC | API | Append |
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Summary
# MAGIC
# MAGIC Bronze layer stores raw and semi-processed data from batch, streaming, and API sources in Delta format on Azure Blob Storage.