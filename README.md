# Olympic Athletes ETL

A reproducible, container-friendly ETL pipeline that scrapes, cleans, validates and transforms Olympic athletes data into dimensional tables and a results fact table. The project includes Airflow orchestration (Docker-ready), data cleaning scripts, reusable scrapers, and staging for Bronze/Silver/Gold data layers.

## Table of contents
- [Project overview](#project-overview)
- [Key features](#key-features)
- [Repository layout](#repository-layout)
- [Data flow](#data-flow)
- [Quick Start](#quick-start)
- [License & contact](#license--contact)


## Project overview
This repository collects Olympic athletes data from public sources, applies a set of cleaning and enrichment steps, and produces a set of analytic-ready tables:

- dim_athletes.parquet — athlete dimension
- dim_affiliations.parquet — affiliation dimension
- bridge_athletes_affiliations.parquet — athlete-affiliation bridge table
- dim_games.parquet — Olympic games dimension
- fct_results.parquet — results fact table

The pipeline emphasizes reproducibility, modular code (scrapers, cleaning, validation), and Airflow orchestration for scheduling and observability. Processed outputs are kept in minio s3 storage `gold/clean_data_final/`.


## Key Features

The Olympic Athletes ETL project offers several professional-grade features that make it robust, auditable, and production-ready:

### 1. Resumable Scraping
- Individual athlete and Olympic edition scrapers are **idempotent**.  
- Can resume from partial runs to avoid data loss during interruptions.  
- Reduces redundant processing and increases pipeline reliability.
- failed IDs are logged and saved which enables easy tracking and re-processing.

### 2. Advanced Data Cleaning & Imputation
- Handles missing values with **domain-specific strategies**:
  - Height/weight imputed by sex and discipline.
  - Born country inferred from NOC codes.
  - Competition dates completed using related fields.
- Imputation flags track all changes for **transparency and auditability**.

### 3. Data Validation & Quality Checks
- **Pandera schemas** enforce strict validation across all datasets:
  - Bios: duplicate names, BMI ranges, birth/death date logic.
  - Affiliations: unique clubs, valid countries.
  - Results: medal-position consistency.
  - Editions: chronological consistency and uniqueness of games.
- Automatically generates **failure-case dataframes** for detailed auditing.

### 4. Data Warehouse Ready
- Columns are **renamed and reordered** to match dimensional modeling conventions (`dim_` and `fct_` tables).  
- Ensures smooth integration into downstream analytics and reporting pipelines.

### 5. Airflow-Orchestrated Pipeline
- DAG structure ensures **sequential, reliable execution**:
  - Scraping → Cleaning → Validation → Formatting → Storage.
- Supports **retries, logging, and observability** for production-grade reliability.

### 6. Cloud-Native Storage
- Uses **MinIO** as S3-compatible storage for raw, cleaned, and final datasets.  
- Enables reproducibility, versioning, and easy integration with other cloud or analytics tools.

> **Summary:** 
The pipeline combines **robust scraping, intelligent cleaning, strict validation, and warehouse-ready formatting** in a fully orchestrated, auditable, and production-ready ETL workflow.


## Repository layout

Top-level files and folders (high level):

- `airflow/` — Docker Compose, Dockerfile, Airflow configuration and DAGs (see `airflow/dags/`)
- `src/` — main Python modules (scraping, cleaning, validation, logger)

- `requirements.txt` — Python dependencies for standalone scripts

Important scripts in `src/`:

- `athlete_scrape.py` — scraper for athlete pages
- `editions_scrap.py` — scraper for editions/games
- `data_clean.py` / `data_clean_II.py` — cleaning & transformation logic
- `data_quality_and_validation.py` — checks and reporting
- `columns_renaming_reordering_and_final_save.py` — final column ordering and exports
- `logger.py` — logger configuration used across modules

## Data Flow

The Olympic Athletes ETL pipeline follows a structured, layered architecture to ensure data reliability, traceability, and clean outputs:

### 1. Bronze Layer: Raw Data Ingestion
- Scraped athlete and Olympic edition data are first stored in the **Bronze bucket**.
- Raw data remains **untouched** to provide a full historical record.
- Supports **resumable scraping** to handle interruptions and avoid data loss.

### 2. Silver Layer: Cleaning and Standardization
- Stage I (`data_clean_I`) performs initial **cleanup and normalization**.
- Stage II (`data_clean_II`) handles advanced imputations for missing values and generates **imputation flags** for auditability.
- **schema validation and data quality check** apply Schema-driven checks (Pandera) and intelligent data validation
- **quality checks** failure cases are also stored here for auditing .

### 3. Gold Layer: Final, Warehouse-Ready Data
- The **column renaming and reordering step** prepares datasets to match dimensional modeling standards.
- Outputs are saved in the **Gold bucket** as fully cleaned, validated, and structured parquet files.
- This layer is ready for **analytics, reporting, or integration with BI tools**.

> **Summary:** 
- Data flows sequentially: **Bronze → Silver → Gold**.
- Each layer improves **data quality, structure, and usability**, ensuring reliable ETL processing from raw scrapes to analytics-ready datasets.


## Quick Start

Follow these steps to get the Olympic Athletes ETL pipeline running locally using Docker and Airflow.

### 1. Build the Airflow Docker Image

Navigate to the `airflow/` directory containing your `Dockerfile` and run:

```bash
docker build -t airflow-2.10.5-extended:1.0 .
```

This will create a custom Airflow image with all required dependencies installed from requirements.txt.

### 2. Launch the Airflow Stack

Start the Airflow environment using Docker Compose:

```bash
docker compose up
```

This will spin up:

* PostgreSQL for Airflow metadata.
* Airflow Webserver and Scheduler.
* Airflow Triggerer.
* MinIO object storage with Bronze, Silver, and Gold buckets.

## 3. Access the Airflow UI

Open your browser at:

http://localhost:8080

Login credentials:

Username: airflow<br>
Password: airflow

## 4. Run the DAG

Locate the DAG named Olympic_Athletes_ETL.<br>
Trigger it manually or let it run on its weekly schedule.<br>
Monitor task progress, logs, and status from the Airflow UI.

## 5. Access the Data

Bronze bucket: Raw scraped data.<br>
Silver bucket: Cleaned and validated datasets.<br>
Gold bucket: Final warehouse-ready datasets after renaming and reordering.


## License & contact

**License**: All Rights Reserved.<br>This code is for personal/offline use only 

Email: ayham.zinedine01@gmail.com<br>
Linkedin: https://www.linkedin.com/in/ayham-zinedine-b8ab18291/