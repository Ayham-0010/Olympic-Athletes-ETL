# Olympic-Athletes-ETL-From-Pandas-to-AWS-Glue
End-to-end Olympic athletes ETL pipeline: scrapes, cleans, validates, and transforms data of 147,000+ athletes into warehouse-ready format following the Bronze/Silver/Gold medallion architecture..

This project demonstrates a full modernization of a legacy Python/Pandas ETL into a cloud-ready, AWS-native Spark pipeline, while still running entirely locally and free.

Originally implemented using Python + Pandas + Airflow + MinIO, the ETL has been rebuilt using PySpark and AWS Glue conventions, emulated with LocalStack.

## Table of contents
- [Pandas Olympic Athletes ETL](#pandas-olympic-athletes-etl)
- [AWS Glue Olympic Athletes ETL](#aws-glue-olympic-athletes-etl)
- [Repository layout](#repository-layout)
- [Data flow](#data-flow)
- [License & contact](#license--contact)

# Pandas Olympic Athletes ETL

This Python + Pandas + Panderas + Airflow + MinIO powered ETL pipeline collects Olympic athletes data from public sources, applies a set of cleaning and enrichment steps, and produces a data set of analytic-ready tables.

**→ [Explore the complete results & data preview here](https://github.com/Ayham-0010/Olympic-Athletes-ETL/blob/main/pandas_based/nootbooks/results_preview.ipynb)**  


## Table of contents
- [Key Features (Pandas)](#key-features-pandas)
- [Quick Start (Pandas)](#quick-start-pandas)

## Key Features (Pandas)

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

## Quick Start (Pandas)

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


# AWS Glue Olympic Athletes ETL
A fully local, production-style AWS Glue (PySpark) ETL pipeline for the Olympic Athletes dataset – distributed scraping, resumable checkpoints, bronze/silver/gold lakehouse, running 100% offline with LocalStack + Glue 5.0 Docker.

## Table of contents

## Table of contents
- [Key Features (AWS Glue / Spark)](#key-features-aws-glue--spark)
- [Design Decisions and Migration Rationale](#design-decisions-and-migration-rationale)
- [Future Improvements](#future-improvements)
- [Quick Start (AWS Glue + LocalStack)](#quick-start-aws-glue--localstack)


# Key Features (AWS Glue / Spark)

<h3>1. Distributed & Resumable Scraping (Spark + RDD Partitions)</h3>

Scraping is now performed using Spark RDD mapPartitions, enabling massively parallel execution.

* Fully distributed scraping using Spark executors.

* Partition-level resumability by checkpointing to S3 (LocalStack) that ensures crash recovery, no redundant scraping, and incremental continuation across runs

* All failures are logged into a dedicated Parquet files for reprocessing.


<h3>2. Spark-Based Data Cleaning & Parsing</h3>

All previous Pandas transformations were rewritten in PySpark, enabling scalable comprehensive data cleaning and imputation.

* Exploding multi-value fields into normalized structures
* Null normalization and string standardization
* Reliable schema enforcement using Spark's StructType/StructField system
* Glue-compatible DataFrame output for downstream modeling

<h3>3. Lakehouse Layout (Bronze → Silver → Gold)</h3>

* Bronze: raw scraped records + checkpoints
* Silver: cleaned, standardized Spark tables
* Gold: dimensional + fact tables ready for analytics

Ensures clean separation, lineage, and reproducibility.

<h3>4. AWS-Native ETL Workflow — Fully Emulated Locally</h3>

The entire pipeline is built using a real AWS Glue job structure, but fully emulated locally using:

* LocalStack for S3 and AWS service endpoints
* AWS Glue 5.0 Docker image (amazon/aws-glue-libs) to simulate Glue jobs and runtime
* No AWS account, no cloud cost

Key behaviors:

* ETL jobs run inside the official AWS Glue 5 Docker image, ensuring runtime parity with Glue’s production environment.

* Spark automatically configured to read/write to the emulated S3

* Airflow remains the orchestrator of the workflow, triggering Glue-style ETL jobs via DockerOperator mirroring enterprise best practices:

  * Airflow DAGs define job schedules and retries
  * DockerOperator launches Glue 5 containers with Spark-submit commands
  * Glue jobs execute the ETL logic against LocalStack S3


This setup lets you develop, test, and run a full AWS-native data pipeline locally, mirroring real Glue + S3 workflows.



# Design Decisions and Migration Rationale

During the migration from the original Pandas-based ETL to an AWS-native PySpark pipeline, several engineering decisions were made to ensure scalability, reliability, and cloud readiness.

**Using RDD + mapPartitions for Parallel Web Scraping**

Although PySpark DataFrames are preferred for almost all transformations, RDDs are the correct choice for I/O-heavy tasks, especially external web scraping.

Why RDD + mapPartitions?

* Web scraping is not vectorizable, so DataFrame APIs provide no benefit.
* Scraping is I/O-bound, not CPU-bound; parallelizing across Spark partitions improves throughput.
* mapPartitions allows one scraper session per partition, reducing overhead and improving efficiency.
* Each partition writes a checkpoint file (to local storage or S3), ensuring resiliency if a partition fails.
* After all partitions complete, Spark consolidates checkpoint outputs into a unified DataFrame for downstream cleaning.

This approach provides safe, fault-tolerant, parallel scraping while staying fully compatible with AWS Glue’s distributed execution model.

**Migration Principles Followed:**

While migrating from Pandas → Spark, the following principles were applied:
* keep transformation semantics identical
* use Spark-native constructs instead of Pandas anti-patterns
* avoid UDFs unless absolutely necessary
* partition intelligently based on athlete ID



# Future Improvements

1. Delta Lake Migration

* Replace Parquet Bronze/Silver/Gold layers with Delta Lake for ACID transactions
* Enable time travel, schema evolution, and scalable upserts
* Improve reliability for incremental refreshes and late-arriving data

2. Data Quality with Great Expectations

* Add automated validation suites for every stage (Bronze → Silver → Gold)
* Generate data documentation and validation reports
* Enforce quality gates before publishing downstream datasets


# Quick Start (AWS Glue + LocalStack)

This section explains how to run the AWS-native Spark ETL pipeline locally, using:

* AWS Glue 5.0 Runtime (Docker image)
* LocalStack (AWS emulator for S3 and more)
* Airflow (with Docker provider) orchestrating Glue-style jobs
* Custom Glue 5 extended image
* No AWS account required.

**1. Pull Required Docker Images**

```bash
docker pull amazon/aws-glue-libs:5
docker pull localstack/localstack
```

**2. Clone the Repository**
git clone https://github.com/Ayham-0010/Olympic-Athletes-ETL.git<br>
cd Olympic-Athletes-ETL/aws_migration

**3. Build the Extended Glue 5 Image**

This installs project requirements on top of the base Glue 5 image:

```bash
docker build -t aws-glue-5-extended:1.0 .
```

**4. Create the External Docker Network**

Your entire environment must run on the same network (Airflow + LocalStack + Glue Spark).

```bash
docker network create aws-net
```

If it already exists:

```bash
docker network inspect aws-net
```

**5. Find Your User and Group IDs (Linux only)**

Airflow must run as your local user so mounted folders aren’t owned by root.

```bash
Check your UID & GID:
id -u
id -g
```

You will use the GID for this line in docker-compose:

```yaml
group_add:
  - <YOUR_GID>
```

Example:

```yaml
group_add:
  - 984
```

**Replace 984 with your actual GID.**

**Note:** Add this under every Airflow service:<br>
airflow-webserver, airflow-scheduler, airflow-triggerer, airflow-worker (if used), and airflow-cli.

**6. Start the Full Stack**

Run Airflow + LocalStack:

```bash
docker compose up
```


Access Airflow:

http://localhost:8080

Username: airflow<br>
Password: airflow

**7. Run the AWS Glue 5 ETL DAG**

In the UI, locate:

**aws_etl_glue5**


Trigger it manually.<br>
This executes:<br>
Glue-style Spark jobs (GlueContext, Job.init, Glue boilerplate)<br>
S3 I/O via LocalStack<br>
Bronze → Silver → Gold ETL workflow

**8. Inspect the Results Using Glue 5 PySpark Shell**
```bash
Run a temporary Glue 5 container inside the same Docker network:

docker run -it --rm \
  --network aws-net \
  -v ~/.aws:/home/hadoop/.aws \
  -e AWS_ACCESS_KEY_ID=test \
  -e AWS_SECRET_ACCESS_KEY=test \
  -e AWS_DEFAULT_REGION=us-east-1 \
  public.ecr.aws/glue/aws-glue-libs:5 \
  pyspark \
    --conf spark.hadoop.fs.s3a.access.key=test \
    --conf spark.hadoop.fs.s3a.secret.key=test \
    --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false
```

Then inside Spark:

```bash
df = spark.read.parquet("s3a://bronze/raw_data/biodata.parquet")
df.show()

df = spark.read.parquet("s3a://gold/clean_data_final/fct_results.parquet")
df.show()
```

**⚠️ LocalStack Warning (Important)**

The free version of LocalStack does NOT persist S3 data.<br>
**Stopping the container deletes everything.**<br>

For persistent storage:<br>
Keep LocalStack running<br>
Or upgrade to LocalStack Pro<br>


## Repository layout

**`pandas_based/`**

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


---

**`aws_migration/`**

- `dags/` — Airflow DAG definitions for AWS Glue ETL pipelines  
- `src/` — AWS Glue job Python modules (scraping, cleaning, transformations, demo job)  
- `docker-compose.yaml` — Docker Compose setup for Airflow, Postgres, and supporting  
- `dockerfile` — Custom Dockerfile for local Airflow + Glue development  

**Important scripts in `src/`:**

- `athlete_scrape_glue.py` — Glue job for scraping athlete data  
- `game_scrape_glue.py` — Glue job for scraping game information  
- `columns_renaming_reordering_glue.py` — Column ordering & renaming logic  
- `data_clean_glue.py` — First-stage data cleaning and transformation  
- `data_clean_II_glue.py` — Secondary cleaning pipeline  
- `glue_demo_job.py` — Minimal end-to-end Glue demo job  


## Data Flow

The Olympic Athletes ETL pipeline follows a structured, layered architecture to ensure data reliability, traceability, and clean outputs:

### 1. Bronze Layer: Raw Data Ingestion
- Scraped athlete and Olympic edition data are first stored in the **Bronze bucket**.
- Raw data remains **untouched** to provide a full historical record.
- Supports **resumable scraping** to handle interruptions and avoid data loss.

### 2. Silver Layer: Cleaning and Standardization
- Stage I (`data_clean_I`) performs initial **cleanup and normalization**.
- Stage II (`data_clean_II`) handles advanced imputations for missing values and generates **imputation flags** for auditability.
- **schema validation and data quality check** apply Schema-driven checks (Pandera) and intelligent data validation **(pandas_based only)**
- **quality checks** failure cases are also stored here for auditing. **(pandas_based only)**

### 3. Gold Layer: Final, Warehouse-Ready Data
- The **column renaming and reordering step** prepares datasets to match dimensional modeling standards.
- Outputs are saved in the **Gold bucket** as fully cleaned, validated, and structured parquet files.
- This layer is ready for **analytics, reporting, or integration with BI tools**.

> **Summary:** 
- Data flows sequentially: **Bronze → Silver → Gold**.
- Each layer improves **data quality, structure, and usability**, ensuring reliable ETL processing from raw scrapes to analytics-ready datasets.


## License & contact

**License**: All Rights Reserved.<br>This code is for personal/offline use only 

Email: ayham.zinedine01@gmail.com<br>
Linkedin: https://www.linkedin.com/in/ayham-zinedine-b8ab18291/
