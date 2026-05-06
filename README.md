# NYC Payroll Data Engineering Project

## Project Overview

This project builds a complete ETL pipeline for NYC Citywide Payroll Data
containing 2.1 million records from 2014 to 2017.

---

## Project Structure

nyc_payroll/
├── airflow/ → pipeline orchestration
│ ├── dags/
│ │ └── nyc_payroll_dag.py → Airflow DAG definition
│ ├── logs/ → Airflow execution logs
│ ├── plugins/ → Airflow plugins
│ └── docker-compose.yml → Docker configuration
├── data/
│ ├── raw/ → source CSV file (2.1M rows)
│ └── new/ → incremental CSV files
├── database/ → database connection
│ ├── **init**.py
│ └── postgresql.py
├── etl/ → ETL pipeline logic
│ ├── **init**.py
│ ├── load_raw.py → extract CSV to raw layer
│ ├── load_stg.py → transform to staging
│ ├── load_final.py → load to warehouse
│ └── load_incremental.py → incremental load logic
├── sql/
│ ├── ddl/ → table creation queries
│ │ ├── 01_create_raw_tables.sql
│ │ ├── 02_create_stg_tables.sql
│ │ ├── 03_create_final_tables.sql
│ │ └── 04_create_batch_tracker.sql
│ └── dml/ → data insertion queries
│ ├── 01_load_raw_tables.sql
│ ├── 02_load_stg_tables.sql
│ └── 03_load_final_tables.sql
├── profiling/ → data profiling queries
│ └── profiling.sql
├── analysis/ → analysis queries
│ └── analysis.sql
├── src/ → utility functions
│ ├── **init**.py
│ └── sql_utils.py
├── main.py → pipeline entry point
└── README.md → project documentation

---

## Dataset

- Source: NYC Citywide Payroll Data
- Total Records: 2,194,488
- Columns: 16
- Years: 2014 - 2017

---

## Data Profiling Findings

- Mid Init: 890,008 NULLs (40.56%) → kept as NULL (optional field)
- Work Location: 506,214 NULLs (23.07%) → replaced with 'UNKNOWN'
- Last/First Name: ~950 NULLs → replaced with 'UNKNOWN'
- Duplicates: 13 records → removed using SELECT DISTINCT
- Salary columns: contain $ sign → removed using REPLACE
- Pay Basis: leading spaces → fixed using TRIM

---

## Star Schema Design

fact_payroll (center)
├── dim_employee → WHO (employee details)
├── dim_agency → WHICH department
├── dim_title → WHAT job title
├── dim_location → WHERE (borough)
├── dim_pay_basis → HOW paid
└── dim_date → WHEN (fiscal year)

### Why Star Schema?

- Fast analytical queries
- No data redundancy
- Easy to understand
- Industry standard for data warehouses

---

## ETL Pipeline

### Extract

- Source: CSV file (2.1 million rows)
- Load into: raw.raw_nyc_payroll
- All columns stored as TEXT
- No transformations applied

### Transform

- Remove $ signs from salary columns
- Convert data types (TEXT → INT, DATE, NUMERIC)
- Handle NULL values with COALESCE
- Remove duplicates with SELECT DISTINCT
- Fix spaces with TRIM

### Load

- Load dimension tables first
- Load fact table last
- Maintain referential integrity

---

## How to Run

### Prerequisites

```bash
# Install required packages
pip install psycopg2 pandas
```

### Setup Database

1. Open pgAdmin
2. Create database: nyc_payroll
3. Update password in database/postgresql.py

### Run Pipeline

```bash
# activate virtual environment
venv\Scripts\activate

# run ETL pipeline
python main.py
```

### Expected Output

Database Connected Successfully
Loading CSV... please wait (2.1 million rows!)
Raw layer loaded!
RAW layer loaded successfully
Staging layer loaded!
STAGING layer loaded successfully
Final layer loaded!
WAREHOUSE layer loaded successfully

---

## Key Findings

### KPIs

- Total Employees: 798,132
- Total Payroll Cost: $117 billion
- Average Base Salary: $41,100
- Total OT Hours: 145 million hours

### Top Insights

- Highest paid employee: Scott Evans ($350,000)
  → Pension Investment Advisor
  → Office of the Comptroller
- Largest agency: DEPT OF ED (423,338 employees)
- Most employees work in MANHATTAN
- Salary increased year over year 2014-2017

---

## Data Profiling

Run `profiling/profiling.sql` in pgAdmin
to see full profiling report

## Analysis Queries

Run `analysis/analysis.sql` in pgAdmin
to see all analytical insights

## Airflow Setup

### Prerequisites

- Docker Desktop installed and running

### Start Airflow

```bash
cd airflow
docker compose up -d
```

### Access Airflow UI

URL: http://localhost:8080
Username: airflow
Password: airflow

### DAG

- DAG ID: nyc_payroll_pipeline
- Schedule: @daily (runs every midnight)
- Tasks: extract → transform → load → incremental
