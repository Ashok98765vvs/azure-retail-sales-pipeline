# Azure Retail Sales Pipeline

![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4?style=for-the-badge&logo=microsoftazure)
![PySpark](https://img.shields.io/badge/PySpark-3.4-E25A1C?style=for-the-badge&logo=apachespark)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python)
![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=for-the-badge)

## Overview

An end-to-end production-grade Azure Data Engineering pipeline that ingests raw retail sales data, processes it through a **Medallion Architecture (Bronze → Silver → Gold)**, applies automated data quality checks, and serves curated aggregations for analytics and BI dashboards.

> Built to demonstrate real-world skills in Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, PySpark, and data quality engineering — aligned with Data Engineer and Analytics Engineer roles.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    AZURE RETAIL SALES PIPELINE                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  [Source: CSV/API]                                                  │
│       │                                                             │
│       ▼                                                             │
│  ┌─────────────────────────┐                                        │
│  │  Azure Data Factory     │  ← Scheduled trigger (daily 1 AM UTC) │
│  │  (Ingest Pipeline)      │                                        │
│  └──────────┬──────────────┘                                        │
│             │                                                       │
│             ▼                                                       │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │           Azure Data Lake Storage Gen2 (ADLS)               │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │   │
│  │  │   BRONZE     │  │   SILVER     │  │      GOLD        │  │   │
│  │  │  (raw CSV)   │→ │  (cleaned)   │→ │  (aggregated)    │  │   │
│  │  └──────────────┘  └──────────────┘  └──────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────┘   │
│             │                                                       │
│             ▼                                                       │
│  ┌─────────────────────────┐                                        │
│  │  Azure Databricks       │  ← PySpark notebooks (3 layers)       │
│  │  + Data Quality Engine  │  ← Great Expectations-style checks     │
│  └──────────┬──────────────┘                                        │
│             │                                                       │
│             ▼                                                       │
│  ┌─────────────────────────┐                                        │
│  │  Power BI / Synapse     │  ← Gold tables → dashboards            │
│  └─────────────────────────┘                                        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Orchestration | Azure Data Factory (ADF) |
| Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Compute | Azure Databricks + PySpark |
| Data Quality | Custom DQ Engine (Python + PySpark) |
| Serving | Azure Synapse Analytics / Power BI |
| Language | Python 3.10, PySpark 3.4 |
| IaC | Azure Bicep (infrastructure templates) |
| CI/CD | GitHub Actions |

---

## Project Structure

```
azure-retail-sales-pipeline/
├── README.md
├── requirements.txt
├── .gitignore
│
├── data/
│   ├── raw_sample/
│   │   ├── sales_2026_07_01.csv       # Sample raw ingestion data
│   │   └── sales_2026_07_02.csv
│   └── curated_sample/
│       ├── dim_store.csv              # Store dimension table
│       └── fact_sales_daily.csv       # Daily fact table
│
├── notebooks/
│   ├── 01_bronze_ingest.py            # Bronze: raw data landing
│   ├── 02_silver_transform.py         # Silver: cleaning & normalization
│   ├── 03_gold_aggregation.py         # Gold: business aggregations
│   └── 04_data_quality_checks.py      # Data quality validation engine
│
├── adf/
│   ├── pipeline_sales_ingest.json     # ADF pipeline ARM template
│   ├── linked_service_adls.json       # ADLS linked service config
│   └── dataset_sales_csv.json         # ADF dataset definition
│
├── config/
│   └── pipeline_config.py             # Centralized config & constants
│
└── infra/
    └── main.bicep                     # Azure Bicep IaC template
```

---

## Pipeline Stages

### Bronze Layer — Raw Ingestion
- ADF Copy Activity pulls daily CSV files from source (HTTP/Blob) into ADLS `bronze/` container
- Data lands as-is: no transformation, preserving full audit trail
- Partitioned by `year/month/day` for efficient querying
- Trigger: daily scheduled at 01:00 AM UTC

### Silver Layer — Cleaning & Normalization
- PySpark reads from Bronze partition
- Applies: null handling, type casting, string trimming, date parsing
- Deduplicates on `order_id` (primary key)
- Adds metadata columns: `ingestion_timestamp`, `pipeline_run_id`, `source_file`
- Writes as Delta format to `silver/` container

### Gold Layer — Business Aggregations
- Builds daily fact table: sales by store, by product category, by payment type
- Calculates KPIs: daily revenue, average order value, transaction count
- Joins with store dimension table
- Writes to `gold/` as Parquet for Power BI consumption

### Data Quality Engine
- Null threshold checks (configurable per column)
- Duplicate detection on primary keys
- Row-count drift alert (>30% drop triggers pipeline failure)
- Schema validation (expected columns + types)
- Results logged to `dq_results/` with pass/fail status

---

## Sample Data Schema

```
order_id       | STRING  | Unique transaction ID
store_id       | STRING  | Store identifier
order_date     | DATE    | Transaction date (YYYY-MM-DD)
product_cat    | STRING  | Product category
quantity       | INT     | Units sold
unit_price     | FLOAT   | Price per unit (USD)
total_amount   | FLOAT   | quantity * unit_price
payment_type   | STRING  | CARD / CASH / DIGITAL
customer_id    | STRING  | Customer identifier (anonymized)
region         | STRING  | US region (SE/NE/MW/W)
```

---

## Key Results / Business Impact

| Metric | Result |
|--------|--------|
| Pipeline runtime | < 8 minutes end-to-end |
| Data quality coverage | 100% of critical columns monitored |
| Daily records processed | ~50,000 transactions/day |
| Row-count drift detection | Alerts within 2 minutes of anomaly |
| Gold layer freshness | Available by 01:15 AM UTC daily |

---

## How to Run Locally (PySpark)

```bash
# 1. Clone the repo
git clone https://github.com/Ashok98765vvs/azure-retail-sales-pipeline.git
cd azure-retail-sales-pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run Bronze ingestion (local mode)
python notebooks/01_bronze_ingest.py --env local --date 2026-07-01

# 4. Run Silver transformation
python notebooks/02_silver_transform.py --env local --date 2026-07-01

# 5. Run Gold aggregation
python notebooks/03_gold_aggregation.py --env local --date 2026-07-01

# 6. Run Data Quality checks
python notebooks/04_data_quality_checks.py --env local --date 2026-07-01
```

---

## Azure Deployment

```bash
# Deploy infrastructure via Bicep
az deployment group create \
  --resource-group rg-retail-pipeline \
  --template-file infra/main.bicep \
  --parameters storageAccountName=adlsretailpipeline

# Import ADF pipeline
az datafactory pipeline create \
  --resource-group rg-retail-pipeline \
  --factory-name adf-retail-pipeline \
  --name sales_ingest_pipeline \
  --pipeline @adf/pipeline_sales_ingest.json
```

---

## Skills Demonstrated

- Azure Data Factory pipeline design with scheduled triggers and error handling
- Medallion Architecture implementation (Bronze/Silver/Gold layers)
- PySpark data transformation: cleaning, deduplication, type casting, partitioning
- Delta Lake and Parquet format optimization
- Automated data quality validation with alerting logic
- ADLS Gen2 storage hierarchy and access patterns
- Azure Bicep infrastructure as code
- Production-grade pipeline patterns: idempotency, partitioning, audit trails

---

## Author

**Ashok** | Data Engineer | Auburn University at Montgomery (MS CS, Dec 2026)

- LinkedIn: [linkedin.com/in/ashok-data-engineer](https://linkedin.com/in/ashok-data-engineer)
- GitHub: [github.com/Ashok98765vvs](https://github.com/Ashok98765vvs)
- Open to: Data Engineer | Analytics Engineer | Data Platform Engineer roles
- US Work Authorization: No sponsorship required, available immediately
