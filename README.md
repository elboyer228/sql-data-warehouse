## SQL Data Warehouse — PostgreSQL 

### What this project is
I followed the full Data With Baraa SQL Data Warehouse project and adapted it to PostgreSQL, running the database in a Docker container for a consistent, isolated environment. Original reference: [DataWithBaraa/sql-data-warehouse-project](https://github.com/DataWithBaraa/sql-data-warehouse-project.git).

This repository showcases building a modern data warehouse from scratch using an ELT approach: load raw data first, then transform inside PostgreSQL to deliver analytics-ready datasets.

### What this brought me
- Greater ease and fluency in SQL, especially PostgreSQL.
- Hands-on practice running PostgreSQL in Docker for reproducible environments.
- End‑to‑end experience designing and implementing a warehouse using ELT.
- A solid foundation for downstream analytics (EDA and advanced analysis).

---

## Architecture at a glance (Medallion: Bronze → Silver → Gold)

- Bronze (raw ingestion)
  - Land source CSVs “as‑is” into staging tables for traceability and auditing.
  - No business rules here; the goal is faithful data capture.

- Silver (cleaned and standardized)
  - Trim whitespace and standardize categorical codes to canonical labels.
  - Deduplicate by business keys, keeping the most recent valid record.
  - Normalize and align identifiers across systems (e.g., remove prefixes/delimiters; derive category/product keys).
  - Convert integers to dates; enforce valid ranges; derive product validity windows.
  - Recompute inconsistent sales/price values and handle null/negative cases safely.
  - Perform basic referential integrity checks across customers, products, and categories.

- Gold (analytics-ready)
  - Expose business-friendly views modeled in a simple star schema.
  - Dimensions (customers, products) and a sales fact table designed for reporting and BI.

The result is a clear separation of concerns: ingest, clean/standardize, and serve analytics, optimized for transparency and maintainability.

---

## What I built (high level)
- Containerized PostgreSQL instance orchestrated with Docker.
- ELT pipelines and SQL transformations executed within PostgreSQL.
- A warehouse modeled with Medallion layers (Bronze/Silver/Gold).
- Lightweight documentation and a structured repo to navigate datasets and SQL scripts.

---

## Repository overview

```
sql-data-warehouse/
├── datasets/             # Raw CSVs (ERP and CRM)
├── scripts/              # SQL for init, Bronze, Silver, Gold
├── docker-compose.yml    # PostgreSQL service and volumes
└── docs/                 # Placeholder for future documentation
```

### Scripts
- `scripts/init.sql`: Creates the project database and the `bronze`, `silver`, and `gold` schemas.
- `scripts/bronze.sql`: Creates raw staging tables and bulk‑loads source CSVs into Bronze.
- `scripts/silver_ddl.sql`: Defines the Silver tables used for cleaned/standardized data.
- `scripts/silver_procedure.sql`: Implements the Silver load procedure (ELT) that cleans, standardizes, converts types, deduplicates, derives keys/dates, and enforces basic integrity.
- `scripts/silver_cleaning.sql`: Standalone reference of Silver transformation logic and sanity checks (useful for exploration/debugging).
- `scripts/silver_quality_checks.sql`: Optional data quality checks in Silver (duplicates, spaces, invalid dates, referential integrity, domain standardization).
- `scripts/gold.sql`: Builds analytics‑ready views for dimensions and the sales fact table in the Gold layer.

---

## What’s next
- Exploratory Data Analysis (EDA) on top of the Gold layer  
  Placeholder link: [EDA Repository](https://github.com/your-username/eda-repo) — to be updated.

- Advanced Data Analytics (KPIs, deeper insights, performance analysis)  
  Placeholder link: [Advanced Analytics Repository](https://github.com/your-username/advanced-analytics-repo) — to be updated.

---

## Credits
This work follows and adapts the excellent project by Data With Baraa: [DataWithBaraa/sql-data-warehouse-project](https://github.com/DataWithBaraa/sql-data-warehouse-project.git).

## License
Based on open educational resources; see original repository for license. This forked/adapted work is shared for educational portfolio purposes.
