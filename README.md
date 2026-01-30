# OpenLedger: Financial Reconciliation Platform

![Dagster](https://img.shields.io/badge/Orchestration-Dagster-blue) ![dbt](https://img.shields.io/badge/Transformation-dbt-orange) ![Snowflake](https://img.shields.io/badge/Warehouse-Snowflake-blue)

OpenLedger is an automated financial reconciliation engine designed to replace manual Excel processes with a modern, scalable data pipeline. It ingests transactional data, enforces strict schemas to guarantee data integrity, and reconciles internal ledgers against external banking settlement files to detect financial discrepancies (fraud, data loss, fee mismatches, etc.).

## Architecture
**ELT Pipeline:** Python (Ingestion) → Snowflake (Storage) → dbt (Transformation) → Dagster (Orchestration)

**Ingestion:** Python was used to generate realistic fintech transactions. Incorrect data was incorporated to ensure that dbt tests were functional.

**Orchestration:** Dagster was used to develop the dependency graph. Python scripts and dbt models were treated as Software-Defined Assets.

**Quality Gates:** Automated circuit breakers were incorporated to stop the pipeline if data quality tests fail (e.g. `not_null`, `unique`).

**Reconciliation:** SQL was used to identify mismatched records between internal and external sources.

## Core Features
**Multi-Asset Orchestration:** Decoupled Ingestion and Transformation layers managed by a single DAG.
**Self-Healing Data:** "Staging" layers sanitize raw chaos data (type casting, null handling) before it hits production tables.
**Observability:** Integrated dbt tests serve as "Data Contracts," ensuring no bad data enters the Fact tables.

## Tech Stack
**Language:** Python 3.10+, SQL (Jinja)
**Warehouse:** Snowflake
**Transformation:** dbt Core
**Orchestration:** Dagster
**Visualization:** Streamlit

## Business Value
In a real-world scenario, this system:
1.  Reduces Close Time b Automates T+1 reconciliation, eliminating month-end crunch.
2.  Prevents Revenue Leakage: Instantly flags "Missing Internal" transactions where money was received but not recorded.
3.  Audit Readiness: Provides full data lineage from source CSV to final Report.