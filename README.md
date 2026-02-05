# OpenLedger: Financial Reconciliation Platform

![Dagster](https://img.shields.io/badge/Orchestration-Dagster-blue) ![dbt](https://img.shields.io/badge/Transformation-dbt-orange) ![Snowflake](https://img.shields.io/badge/Warehouse-Snowflake-blue)

OpenLedger is an automated financial reconciliation engine designed to replace manual Excel processes with a modern, scalable data pipeline. It ingests transactional data, enforces strict schemas to guarantee data integrity, and reconciles internal ledgers against external banking settlement files to detect financial discrepancies (fraud, data loss, fee mismatches, etc.).

## Architecture
**ELT Pipeline:** Python (Ingestion) → Snowflake (Storage) → dbt (Transformation) → Dagster (Orchestration)

* Python was used to generate realistic fintech transactions. Incorrect data was artificially incorporated to ensure that dbt tests were functional.
* Dagster was used to develop the dependency graph. Python scripts and dbt models were treated as Software-Defined Assets.
* Automated circuit breakers were incorporated to stop the pipeline if data quality tests fail (e.g. `not_null`, `unique`). For purposes of demonstration, the severity of the circuit breakers were kept as 'warn' rather than 'error.
* SQL was used to identify mismatched records between internal and external sources.

## Core Features
* Decoupled ingestion and transformation layers managed by a single DAG. This allows for fault isolation (makes debugging easier), makes changing the business logic easier, and creates an identifiable audit trail.
* Used staging layers to clean up raw data (type casting, null handling) before it hits production tables.
* Integrated dbt tests serve as enforce data contracts which prevent low-quality data from entering the fact tables (here I just placed the severity as "warn" to show that the process functions end-to-end).

## Tech Stack
* Python
* Snowflake
* dbt Core
* SQL
* Jinja
* Dagster
* Streamlit

## Business Value
In a real-world scenario, this system:
1.  Automates T+1 reconciliation which reduces close-time and eliminates month-end crunch.
2.  Prevents revenue leakage by instantly flagging transactions where money was received but not recorded.
3.  Provides full data lineage from ingestion source to the final report, ensuring audit compliance.