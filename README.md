# Medallion Architecture Data Blueprint

**End-to-End ETL Pipeline Documentation for the Atlon & Sports Bar Integration**

---

## Overview

This project implements a **Medallion Architecture** (Bronze → Silver → Gold) using **Databricks Delta Lake** to unify data from two very different sources:

- **Atlon (Parent Company)** — A mature ERP system with predictable reporting cycles, integer-based master reference keys, and monthly aggregated fact tables.
- **Sports Bar (Child Startup)** — A hyper-growth environment characterized by daily granular CSV/XLSX files with missing historical chunks, dirty alphanumeric keys, inconsistent spelling, negative prices, and missing dimensional attributes.

The goal is a **unified analytical dashboard** for supply chain and forecasting without disrupting Atlon's established reporting.

---

## Pipeline Architecture

### Data Flow

```
CSV/TXT Files → AWS S3 Landing Zone → Delta Lake → Bronze → Silver → Gold → Analytics & Genie
```

| Layer | Role | Key Operations |
|-------|------|----------------|
| **AWS S3 Landing Zone** | Receives daily CSV appends from Sports Bar sources | Incremental file ingestion |
| **Bronze (Raw Ingestion)** | Append-only raw ingestion preserving exact source schemas | `read_timestamp` metadata added; schema-on-read; full history in native format |
| **Silver (Cleansed & Conformed)** | Deduplication, regex cleaning, surrogate key assignment, data type casting | Filtered, augmented, normalized data model |
| **Gold (Aggregated & Business-Ready)** | Daily-to-monthly aggregation, BI-ready metrics, parent-child schema alignment | Business metrics, aggregations, star schema |
| **Analytics & Genie** | Unified parent and child BI insights | Denormalized analytical view for dashboards |

---

## Data Models

### Dimension: `dim_customers`

- **Source:** `S3:/customers/*.csv`
- **Bronze columns:** `customer_id` (int), `customer_name`, `city`
- **Silver transformations:**
  - Drop duplicates on `customer_id`
  - Trim spaces, apply `initcap` on names
  - Standardize city names (e.g., Bangaluru → Bangalore)
  - Resolve null cities via customer group lookup (e.g., Zenith Foods → New Delhi)
  - Cast `customer_id` to String
- **Gold output:**
  - Concatenate `customer_name + '-' + city` → `customer`
  - Rename `customer_id` → `customer_code`
  - Add hardcoded constants: `market = 'India'`, `platform = 'Sports Bar'`, `channel = 'Acquisition'`

### Dimension: `dim_products`

- **Source:** `S3:/products/*.csv`
- **Silver transformations:**
  - Drop exact duplicates
  - Regex replace for case normalization (e.g., `(?i)protein` → `Protein`)
  - Map `category` → `division`
  - Default invalid IDs to `9999`
- **Gold output columns:** `product_code`, `product_id`, `division`, `category`, `product`, `variant`
- **Surrogate Key Generation:**
  - Variant extracted via regex (e.g., "Energy Bar 60g" → variant: `60g`)
  - `product_code` generated via `SHA(product_name)` hash — bypasses unreliable source IDs entirely

### Dimension: `dim_gross_price`

- **Source:** `S3:/gross_price/*.csv`
- **Silver transformations:**
  - `LEFT JOIN dim_products` on `product_id` to fetch `product_code`
  - `try_to_date(date)` for date parsing
  - `abs(price)` to fix negative prices
  - `coalesce(unknown, 0)` for null handling
- **Gold output:**
  - Add `is_zero` flag
  - Extract `year` from date
- **Chronology resolution:** Window function partitioned by `product_code, year`, ordered by `month DESC`, filtered to `Rank = 1` — the latest month price becomes the **Standard Yearly Price**

### Fact: `fact_orders`

#### Bronze Ingestion
- **Source:** `S3:/landing/*.csv` (incremental appends)
- Post-ingestion, files are moved to `/processed/` via `dbutils.fs.mv`

#### Silver Refinery (4 Chambers)

| Chamber | Name | Operations |
|---------|------|------------|
| 1 | Filter Conditions | Drop null `order_quantity`; drop exact row duplicates |
| 2 | String Manipulation | Strip weekday text from dates via regex, then cast with `try_to_date()` |
| 3 | Data Safety | Coalesce non-numeric Customer IDs to `9999` |
| 4 | Join Conditions | `LEFT JOIN dim_products` on `product_id` to attach master `product_code` |

#### Gold Aggregation
- Daily transactions are funneled through `month_start` to roll up to monthly granularity
- **Aggregate:** `SUM(sold_quantity)`
- **Group by:** `month_start`, `product_code`, `customer_code`
- Output: A monthly rolled-up fact table formatted for the master parent merge

---

## Parent-Child Schema Merge (Upsert Engine)

Sports Bar gold tables (child) and Atlon master gold tables (parent) are merged using a `MERGE INTO` operation:

- **Match keys:** `customer_code`, `product_code`
- **`WHEN MATCHED`** → `UPDATE` target attributes
- **`WHEN NOT MATCHED`** → `INSERT` new record

**Idempotent Pipeline Guarantee:** If the pipeline re-runs, existing data is strictly updated. Historical records are never duplicated.

---

## System Orchestration & Scalability

### Execution Order

A **cron job** triggers daily at **11:00 PM** (post-business hours):

1. `Dim Customers` and `Dim Products` run in parallel
2. `Dim Pricing` runs next (requires generated Product Key)
3. `Fact Orders` runs last (requires all dimensions updated first)

### Scalability Mechanism

Isolated staging tables are dynamically created for incremental daily runs. This strictly prevents the costly re-processing of historical data — only the new day's data is processed each run.

---

## Final Output: Denormalized Analytical View

The Medallion architecture culminates in a single, flat analytical view built as a **star schema** with `fact_orders` at the center, joined to:

- `dim_customers`
- `dim_products`
- `dim_gross_price`
- `dim_date`

**Calculated measure:** `sold_quantity × gross_price = total_revenue`

This denormalized view enables **Databricks AI Genie** and **BI Dashboards** to query massive datasets without expensive runtime joins.
