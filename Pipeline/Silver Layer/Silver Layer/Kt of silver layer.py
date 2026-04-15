# Databricks notebook source
# MAGIC %md
# MAGIC # KT — Silver Layer Data Cleaning
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC All 10 bronze tables were cleaned, standardized, and saved as Delta tables in the Silver layer.  
# MAGIC **Bronze layer is read-only** — no modifications made to source data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Processing Flow
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
# MAGIC │   BRONZE LAYER  │     │   CLEANING PIPELINE  │     │  SILVER LAYER   │
# MAGIC │   (Read-Only)   │────▶│                      │────▶│  (Delta Tables) │
# MAGIC │                 │     │  1. Load from Azure  │     │                 │
# MAGIC │  Azure Blob     │     │  2. Null checks      │     │  Azure Blob     │
# MAGIC │  Delta Tables   │     │  3. Duplicate checks  │     │  Delta Tables   │
# MAGIC │                 │     │  4. Validation        │     │                 │
# MAGIC │                 │     │  5. Standardization   │     │  + Metadata     │
# MAGIC │                 │     │  6. Add metadata      │     │    columns      │
# MAGIC │                 │     │  7. Save (overwrite)  │     │                 │
# MAGIC └─────────────────┘     └──────────────────────┘     └─────────────────┘
# MAGIC                                │
# MAGIC                                ▼ (if applicable)
# MAGIC                         ┌──────────────┐
# MAGIC                         │  Bad Records │
# MAGIC                         │  (Quarantine)│
# MAGIC                         └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Storage
# MAGIC
# MAGIC | Item | Value |
# MAGIC |------|-------|
# MAGIC | Storage Account | `capstonestorageaccount03` |
# MAGIC | Container | `capstone-project-pks` |
# MAGIC | Bronze Path | `.../bronze/<table_name>` |
# MAGIC | Silver Path | `.../silver/<table_name>` |
# MAGIC | Format | Delta |
# MAGIC | Write Mode | Overwrite |
# MAGIC | Auth | `dbutils.secrets.get(scope="capstosne-scope", key="storagekey")` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Table-wise Cleaning Summary
# MAGIC
# MAGIC | # | Table | Bronze Rows | Silver Rows | Removed | Key Cleaning Actions |
# MAGIC |---|-------|-------------|-------------|---------|----------------------|
# MAGIC | 1 | **orders** | 99,441 | 99,252 | 189 | Removed invalid date sequences (e.g., delivery before purchase) |
# MAGIC | 2 | **customers** | 99,441 | 99,441 | 0 | No nulls/duplicates; standardized city (InitCap), state (UPPER) |
# MAGIC | 3 | **order_items** | 112,821 | 112,821 | 0 | No nulls/duplicates; validated price & freight >= 0 |
# MAGIC | 4 | **products** | 32,951 | ~32,600 | ~351 | Null `product_category_name` → quarantined to `product_bad_records`; lowercase category |
# MAGIC | 5 | **sellers** | 3,095 | 3,095 | 0 | Cleaned dirty city names (e.g., "lages - sc" → "Lages"); InitCap city, UPPER state |
# MAGIC | 6 | **reviews** | 100,000 | ~99,900 | ~100 | Removed null `review_id`/`order_id`; deduped on `review_id`; lowercase comments |
# MAGIC | 7 | **payments** | 104,024 | 104,024 | 0 | No nulls/duplicates; validated values >= 0; UPPER `payment_type` |
# MAGIC | 8 | **category_translation** | 71 | 71 | 0 | No nulls/duplicates; lowercase both columns |
# MAGIC | 9 | **weather** | ~50,000+ | ~45,000+ | ~5,000+ | Removed nulls, deduped on (lat,lon,date), filtered invalid coords (Brazil bounds), rounded coords to 4dp |
# MAGIC | 10 | **geolocation** | 1,000,163 | 27,911 | 972,252 | Deduped by (zip, city, state) with AVG(lat/lng); UPPER city & state |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Common Cleaning Steps (All Tables)
# MAGIC
# MAGIC 1. **Null Check** — counted nulls per column
# MAGIC 2. **Duplicate Check** — grouped by primary/composite key
# MAGIC 3. **Standardization** — `trim()`, `upper()`/`lower()`/`initcap()` on text columns
# MAGIC 4. **Metadata Added** — `create_date`, `update_date`, `file_location` (or `ingestion_timestamp`, `source`, `layer`)
# MAGIC 5. **Delta Overwrite** — saved to silver path
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Special Cases
# MAGIC
# MAGIC | Table | Special Handling |
# MAGIC |-------|------------------|
# MAGIC | **products** | Bad records (null category) quarantined to `silver/product_bad_records` with `error_reason`, `error_timestamp`, `file_location` |
# MAGIC | **orders** | Nulls in date columns **retained** (reflect real business states: cancelled, pending) |
# MAGIC | **orders** | Invalid date sequences removed (delivery before purchase, approval before purchase) |
# MAGIC | **orders** | VACUUM with 0-hour retention run post-save |
# MAGIC | **weather** | Coordinate validation against Brazil bounds (lat: -34 to 6, lon: -74 to -35) |
# MAGIC | **weather** | Coordinates rounded to 4 decimal places for consistent joins |
# MAGIC | **geolocation** | Massive dedup (1M → 28K) via GROUP BY + AVG on coordinates |
# MAGIC | **sellers** | Regex cleanup of dirty city names (removed state suffix like " - sc") |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Metadata Columns Added
# MAGIC
# MAGIC **Pattern A** (products, sellers, reviews, payments-std, category_translation, weather, geolocation):
# MAGIC - `create_date` — `current_timestamp()`
# MAGIC - `update_date` — `current_timestamp()`
# MAGIC - `file_location` — bronze source path
# MAGIC
# MAGIC **Pattern B** (customers, order_items):
# MAGIC - `ingestion_timestamp` — `current_timestamp()`
# MAGIC - `source` — e.g., `"bronze_customers"`
# MAGIC - `layer` — `"silver"`
# MAGIC
# MAGIC **Pattern C** (orders):
# MAGIC - No metadata columns added (saved as-is after cleaning)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Notebooks Reference
# MAGIC
# MAGIC | Table | Notebook |
# MAGIC |-------|----------|
# MAGIC | orders | `Silver Layer/Orders Table` |
# MAGIC | customers | `Silver Layer/Customer Table` |
# MAGIC | order_items | `Silver Layer/Order_items Table` |
# MAGIC | products | `Silver Layer/Products Table` |
# MAGIC | sellers | `Silver Layer/Seller Tables` |
# MAGIC | reviews | `Silver Layer/Reviews Table` |
# MAGIC | payments | `Silver Layer/Payments Table` |
# MAGIC | category_translation | `Silver Layer/Category Translation table` |
# MAGIC | weather | `Silver Layer/Weather table` |
# MAGIC | geolocation | `Silver Layer/geolocation Table` |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC