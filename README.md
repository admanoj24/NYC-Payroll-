<div align="center">

# 🗽 NYC Citywide Payroll — Data Engineering Pipeline

<img src="https://img.shields.io/badge/Records-2.1M-blue?style=for-the-badge&logo=databricks&logoColor=white"/>
<img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white"/>
<img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>
<img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/>
<img src="https://img.shields.io/badge/Years-2014--2018-orange?style=for-the-badge"/>

<br/>
<br/>

> **An end-to-end ETL pipeline** that ingests, transforms, and warehouses 2.1 million records of NYC employee payroll data (2014–2017 full load + 2018 incremental) into a star-schema data warehouse — orchestrated with Apache Airflow.

</div>

---

## 📌 Table of Contents

- [📊 Project Overview](#-project-overview)
- [🔢 Key Metrics at a Glance](#-key-metrics-at-a-glance)
- [🗂️ Project Structure](#️-project-structure)
- [🧹 Data Profiling Findings](#-data-profiling-findings)
- [⭐ Star Schema Design](#-star-schema-design)
- [⚙️ ETL Pipeline](#️-etl-pipeline)
- [🚀 How to Run](#-how-to-run)
- [📈 Key Findings & Insights](#-key-findings--insights)
- [🌬️ Airflow Orchestration](#️-airflow-orchestration)

---

## 📊 Project Overview

This project builds a **production-ready ETL data pipeline** for NYC Citywide Payroll data. It follows the **Medallion Architecture** (Raw → Staging → Warehouse) and uses a **Star Schema** optimized for fast analytical queries.

| Property | Value |
|---|---|
| 📁 Source | NYC Open Data — Citywide Payroll (Kaggle) |
| 🗄️ Database | PostgreSQL |
| 🔢 Total Records | 2,194,488 |
| 📅 Years Covered | 2014 – 2018 (Full Load: 2014–2017 + Incremental: 2018) |
| 🧱 Architecture | Medallion (Raw → Staging → Warehouse) |
| 🌐 Orchestration | Apache Airflow (Dockerized) |

---

## 🔢 Key Metrics at a Glance

<div align="center">

| 💼 Total Employees | 💰 Total Payroll Cost | 📊 Avg Base Salary | ⏱️ Total OT Hours |
|:---:|:---:|:---:|:---:|
| **798,137** | **$117 Billion** | **$41,100** | **145 Million hrs** |

</div>

---

## 🗂️ Project Structure

```
nyc_payroll/
│
├── 📁 airflow/                     → Pipeline orchestration
│   ├── dags/
│   │   └── nyc_payroll_dag.py      → Airflow DAG definition
│   ├── logs/                       → Airflow execution logs
│   ├── plugins/                    → Airflow plugins
│   └── docker-compose.yml          → Docker configuration
│
├── 📁 data/
│   ├── raw/                        → Source CSV file (2.1M rows)
│   └── new/                        → Incremental CSV files
│
├── 📁 database/                    → Database connection
│   ├── __init__.py
│   └── postgresql.py
│
├── 📁 etl/                         → ETL pipeline logic
│   ├── __init__.py
│   ├── load_raw.py                 → Extract CSV → Raw layer
│   ├── load_stg.py                 → Transform → Staging layer
│   ├── load_final.py               → Load → Warehouse layer
│   └── load_incremental.py         → Incremental load logic
│
├── 📁 sql/
│   ├── ddl/                        → Table creation queries
│   │   ├── 01_create_raw_tables.sql
│   │   ├── 02_create_stg_tables.sql
│   │   ├── 03_create_final_tables.sql
│   │   └── 04_create_batch_tracker.sql
│   └── dml/                        → Data insertion queries
│       ├── 01_load_raw_tables.sql
│       ├── 02_load_stg_tables.sql
│       └── 03_load_final_tables.sql
│
├── 📁 profiling/
│   └── profiling.sql               → Data profiling queries
│
├── 📁 analysis/
│   └── analysis.sql                → Analytical insight queries
│
├── 📁 src/                         → Utility functions
│   ├── __init__.py
│   └── sql_utils.py
│
├── main.py                         → Pipeline entry point
└── README.md                       → Project documentation
```

---

## 🧹 Data Profiling Findings

| Column | Issue Found | Action Taken |
|---|---|---|
| `Mid Init` | 890,008 NULLs (40.56%) | Kept as NULL (optional field) |
| `Work Location` | 506,214 NULLs (23.07%) | Replaced with `'UNKNOWN'` |
| `Last / First Name` | ~950 NULLs | Replaced with `'UNKNOWN'` |
| `Salary Columns` | Contains `$` signs | Removed using `REPLACE()` |
| `Pay Basis` | Leading whitespace | Fixed using `TRIM()` |
| `Duplicates` | 13 duplicate records | Removed using `SELECT DISTINCT` |

> 🔍 Run `profiling/profiling.sql` in pgAdmin for the complete data profiling report.

---

## ⭐ Star Schema Design

```
                        ┌─────────────────┐
                        │   dim_employee  │  → WHO
                        └────────┬────────┘
                                 │
  ┌─────────────┐       ┌────────▼────────┐       ┌──────────────┐
  │  dim_agency │  ───► │  fact_payroll   │ ◄───  │  dim_title   │
  └─────────────┘       └────────┬────────┘       └──────────────┘
  (WHICH dept)                   │                  (WHAT job)
                    ┌────────────┼────────────┐
                    │            │            │
             ┌──────▼───┐  ┌────▼────┐  ┌───▼────────┐
             │dim_location│ │dim_date │  │dim_pay_basis│
             └──────────┘  └─────────┘  └────────────┘
              (WHERE)        (WHEN)         (HOW)
```

### Why Star Schema?

- ⚡ **Fast analytical queries** — minimal joins needed
- 🔁 **No data redundancy** — dimensions stored once
- 👁️ **Easy to understand** — business-friendly structure
- 🏭 **Industry standard** — battle-tested for data warehouses

---

## ⚙️ ETL Pipeline

```
┌──────────────────────────────────────────────────────────────┐
│                        ETL PIPELINE                          │
│                                                              │
│  📥 EXTRACT         🔄 TRANSFORM          📤 LOAD           │
│  ─────────          ───────────          ──────              │
│  CSV File     →    Remove $ signs   →   dim tables first     │
│  2.1M rows         Type casting          fact table last     │
│  TEXT only         NULL handling         Referential         │
│                    Dedup (DISTINCT)       integrity          │
│                    TRIM spaces           maintained          │
└──────────────────────────────────────────────────────────────┘
```

| Phase | Source | Destination | Key Operations |
|---|---|---|---|
| **Extract** | CSV (2.1M rows) | `raw.raw_nyc_payroll` | Load as TEXT, no transforms |
| **Transform** | Raw layer | `stg.stg_nyc_payroll` | Type casting, NULL handling, dedup, cleaning |
| **Load** | Staging layer | Final warehouse tables | Star schema load, referential integrity |
| **Incremental** | New CSV files | Warehouse | Batch tracking, ON CONFLICT DO NOTHING |

---

## 🛠️ Technologies Used

| Tool | Purpose |
|---|---|
| Python | ETL pipeline development |
| PostgreSQL | Data warehouse storage |
| psycopg2 | Python-PostgreSQL connection |
| Apache Airflow | Pipeline orchestration |
| Docker | Airflow containerization |
| pgAdmin | Database management |

---

## 🚀 How to Run

### 1️⃣ Prerequisites

```bash
pip install psycopg2
```

### 2️⃣ Setup Database

1. Open **pgAdmin**
2. Create a database named: `nyc_payroll`
3. Update your password in `database/postgresql.py`

### 3️⃣ Place CSV File

```
data/raw/Citywide_Payroll_Data__Fiscal_Year_.csv
```

### 4️⃣ Run the Full Load Pipeline

```bash
# Activate your virtual environment
venv\Scripts\activate        # Windows
source venv/bin/activate     # macOS / Linux

# Run the ETL pipeline
python main.py
```

### ✅ Expected Output

```
Database Connected Successfully
Loading CSV... please wait (2.1 million rows!)
Raw layer loaded!
RAW layer loaded successfully
Staging layer loaded!
STAGING layer loaded successfully
Final layer loaded!
WAREHOUSE layer loaded successfully
Batch 1 started!
Incremental raw loaded! (5 rows processed)
Incremental staging loaded!
Incremental final layer loaded!
Batch 1 completed successfully!
```

### 5️⃣ Run Data Profiling

```
Open profiling/profiling.sql in pgAdmin
Run queries one by one
```

### 6️⃣ Run Analysis Queries

```
Open analysis/analysis.sql in pgAdmin
Run queries one by one
```

---

## 📈 Key Findings & Insights

### 🏆 Top Findings

| Insight | Detail |
|---|---|
| 👑 Highest Paid Employee | **Scott Evans** — $350,000/yr |
| 🏢 Highest Paid Role | Pension Investment Advisor, Office of the Comptroller |
| 🏫 Largest Agency | **DEPT OF ED** — 423,338 employees |
| 📍 Most Common Location | **Manhattan** — 534,697 employees |
| 📈 Salary Trend | Increased every year from 2014 → 2018 |
| ⏱️ Most OT | NYPD has highest overtime hours |

### 📊 Summary KPIs

```
💼 Total Employees    →  798,132
💰 Total Payroll      →  $117,000,000,000
📊 Avg Base Salary    →  $41,100
⏱️ Total OT Hours     →  145,000,000 hours
🏢 Total Agencies     →  165
👔 Total Job Titles   →  1,963
```

> 📂 Run `analysis/analysis.sql` in pgAdmin for the full analytical breakdown.

---

## 🌬️ Airflow Orchestration

### Prerequisites

- ✅ Docker Desktop installed and running

### Start Airflow

```bash
cd airflow
docker compose up -d
```

### Access the Airflow UI

| Setting | Value |
|---|---|
| 🌐 URL | `http://localhost:8080` |
| 👤 Username | `airflow` |
| 🔑 Password | `airflow` |

### DAG Overview

| Property | Value |
|---|---|
| DAG ID | `nyc_payroll_pipeline` |
| Schedule | `@daily` (runs every midnight automatically) |
| Task 1 | `extract_raw_layer` — Load CSV to raw table |
| Task 2 | `transform_stg_layer` — Clean data in staging |
| Task 3 | `load_warehouse_layer` — Load star schema |
| Task 4 | `incremental_load` — Load new records only |
| Task Flow | `extract → transform → load → incremental` |

### Stop Airflow

```bash
docker compose down
```

---

## 📦 Dataset

- **Source:** [NYC Citywide Payroll Data — Kaggle](https://www.kaggle.com/datasets/new-york-city/nyc-citywide-payroll-data)
- **Records:** 2,194,488
- **Columns:** 16
- **Years:** 2014 — 2018

---

<div align="center">

**Built with ❤️ by Manoj Adhikari using Python · PostgreSQL · Apache Airflow · Docker**

</div>
