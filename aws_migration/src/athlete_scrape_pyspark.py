import sys
import os
import time
import random
import traceback
from io import StringIO
from datetime import datetime
from typing import Iterator, List, Dict, Tuple

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, ArrayType)
from pyspark.sql import Row

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# ----------------------------
# SETTINGS (change to taste)
# ----------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
JOB_NAME = args["JOB_NAME"]

SIZE = 64 #150000
RETRY_NUM = 3
# Partition parallelism - tune to number of cores you want to use locally
NUM_PARTITIONS = 16

# Checkpoint folder (on MinIO / LocalStack)
BRONZE_BUCKET = "s3a://bronze"
CHECKPOINTS_PREFIX = f"{BRONZE_BUCKET}/checkpoints"
RAW_BIODATA_PATH = f"{BRONZE_BUCKET}/raw_data/biodata.parquet"
RAW_RESULTS_PATH = f"{BRONZE_BUCKET}/raw_data/results.parquet"

# MinIO / S3 config
S3_ENDPOINT = "http://localstack:4566"
ACCESS_KEY = "test"
SECRET_KEY = "test"

# ----------------------------
# Glue / Spark init
# ----------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

# Configure S3 for LocalStack/MinIO
spark.conf.set("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
spark.conf.set("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
spark.conf.set("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")  # For MinIO compatibility
spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def biodata_from_soup(soup: BeautifulSoup, athlete_id: int) -> Dict:
    table = soup.find("table", {"class": "biodata"})
    if not table:
        raise ValueError(f"No biodata table for athlete {athlete_id}")
    # Roles,Sex,Full name,Used name,Born,Measurements,Affiliations,NOC,Athlete_Id,Died,Nick/petnames,Title(s),Other names,Nationality,Original name,Name order
    # Using pandas read_html to preserve the same behavior
    df = pd.read_html(StringIO(str(table)), index_col=0)[0]
    df = df.T
    df['Athlete_Id'] = athlete_id

    # convert to dict with string values
    out = {k: (None if pd.isna(v) else str(v)) for k, v in df.iloc[0].to_dict().items()}
    out["Athlete_Id"] = int(athlete_id)
    return out

def results_from_soup(soup: BeautifulSoup, athlete_id: int) -> List[Dict]:
    table = soup.find("table", {"class": "table"})
    if not table:
        # Return empty list if no results
        return []
    # Games,Event,Team,Pos,Medal,As,NOC,Discipline,Athlete_Id,Nationality,Unnamed: 7
    df = pd.read_html(StringIO(str(table)))[0]
    # Create expected columns if missing
    for col in ["NOC / Team", "Discipline (Sport) / Event", "As", "Games"]:
        if col not in df.columns:
            df[col] = None

    # Add columns
    df['NOC'] = None
    df['Discipline'] = None
    df['Athlete_Id'] = athlete_id

    rows_with_meta = df.index[~df['Games'].isna()].tolist()
    df.loc[rows_with_meta, "NOC"] = df.loc[rows_with_meta, "NOC / Team"]
    df.loc[rows_with_meta, "Discipline"] = df.loc[rows_with_meta, "Discipline (Sport) / Event"]
    df.rename(columns={'NOC / Team': 'Team', 'Discipline (Sport) / Event': 'Event'}, inplace=True)
    column_to_ffill = ['NOC', 'Discipline', 'As', 'Games']
    df[column_to_ffill] = df[column_to_ffill].ffill()
    df.drop(columns=['Unnamed: 6'], errors='ignore', inplace=True)
    df = df.drop(rows_with_meta)

    # Convert rows to dicts with string values
    out = []
    for _, r in df.iterrows():
        d = {c: (None if pd.isna(r[c]) else str(r[c])) for c in df.columns}
        d['Athlete_Id'] = int(athlete_id)
        out.append(d)
    return out




# ----------------------------
# List of columns
# ----------------------------

BIODATA_COLUMNS = [
    "Roles", "Sex", "Full name", "Used name", "Born", "Died",
    "Measurements", "Nick/petnames", "Title(s)", "Other names",
    "Original name", "Name order", "Nationality",
    "Affiliations", "NOC",
    "Athlete_Id"
]

RESULTS_COLUMNS = [
    "Games", "Event", "Team", "Pos", "Medal", "As",
    "NOC", "Discipline", "Nationality", "Athlete_Id"
]

# ----------------------------
# Utility Functions
# ----------------------------

def enforce_biodata_schema(raw: Dict) -> Dict:
    """Return a dict that contains ONLY BIODATA_COLUMNS and fills missing fields with None."""
    clean = {}
    for col in BIODATA_COLUMNS:
        clean[col] = None if col not in raw else raw[col]
    return clean


def enforce_results_schema(raw: Dict) -> Dict:
    """Return a dict that contains ONLY RESULTS_COLUMNS and fills missing fields with None."""
    clean = {}
    for col in RESULTS_COLUMNS:
        clean[col] = None if col not in raw else raw[col]
    return clean



# ----------------------------
# Partition scraping function
# ----------------------------
def scrape_partition(ids_iter: Iterator[int]) -> Iterator[Tuple[str, Dict]]:
    """
    For each partition:
      - create a requests.Session()
      - iterate ids and fetch pages, parse biodata and results
      - yield rows as tuples: ('biodata', dict) or ('results', dict)
    """
    session = requests.Session()
    session.headers.update({"User-Agent": "OlympediaScraper/1.0 (+your-email@gmail.com)"})
    base_url = "https://www.olympedia.org/athletes"

    # Convert iterator to list because we may need length and repeated access
    ids = list(ids_iter)
    if not ids:
        return
    for athlete_id in ids:
        time.sleep(random.uniform(0.5, 1.5))  # per-id randomized delay
        url = f"{base_url}/{athlete_id}"
        response = None
        last_exc = None
        for attempt in range(RETRY_NUM):
            try:
                r = session.get(url, timeout=60)
                if r.status_code == 200:
                    response = r
                    break
                else:
                    last_exc = Exception(f"Status {r.status_code}")
            except Exception as e:
                last_exc = e
            time.sleep(1 + attempt * 2)
        if response is None:
            # yield an error row so driver can record failures if desired
            yield ("error", {"Athlete_Id": athlete_id, "error_message": str(last_exc)})
            continue

        try:
            soup = BeautifulSoup(response.content, "html.parser")
            try:
                bio = biodata_from_soup(soup, athlete_id)
                yield ("biodata", enforce_biodata_schema(bio))
            except Exception as e:
                # missing biodata -> error
                yield ("error", {"Athlete_Id": athlete_id, "error_message": f"biodata_error: {e}"})

            try:
                results = results_from_soup(soup, athlete_id)
                # results may be many rows
                for r in results:
                    yield ("results", enforce_results_schema(r))
            except Exception as e:
                # missing results -> still continue
                yield ("error", {"Athlete_Id": athlete_id, "error_message": f"results_error: {e}"})

        except Exception as e:
            yield ("error", {"Athlete_Id": athlete_id, "error_message": f"parse_error: {e}"})

# ----------------------------
# Read existing checkpoints to compute scraped IDs
# ----------------------------
def read_scraped_ids(spark_session):
    """
    Read any existing checkpoint parquet files under CHECKPOINTS_PREFIX and return a set of scraped athlete IDs.
    We'll try both biodata and results checkpoint files. If none exist, return empty set.
    """
    scraped = set()
    try:
        # Try reading any Parquet files in the checkpoints folder
        # We assume checkpoint files have an Athlete_Id column for biodata and results
        df = spark_session.read.parquet(CHECKPOINTS_PREFIX + "/*")
        if "Athlete_Id" in df.columns:
            
            # scraped = set([int(x) for x in df.select("Athlete_Id").distinct().rdd.flatMap(lambda x: x).collect()])
            scraped = set({int(row.Athlete_Id) for row in df.select("Athlete_Id").distinct().collect()})

    except Exception as e:
        # no checkpoints or no files - treat as empty
        pass
    return scraped


# ----------------------------
# Define schemas
# ----------------------------

BIODATA_SCHEMA = StructType([
    StructField("Roles",          StringType(), True),
    StructField("Sex",            StringType(), True),
    StructField("Full name",      StringType(), True),
    StructField("Used name",      StringType(), True),
    StructField("Born",           StringType(), True),
    StructField("Died",           StringType(), True),
    StructField("Measurements",   StringType(), True),
    StructField("Nick/petnames",  StringType(), True),
    StructField("Title(s)",       StringType(), True),
    StructField("Other names",    StringType(), True),
    StructField("Original name",  StringType(), True),
    StructField("Name order",     StringType(), True),
    StructField("Nationality",    StringType(), True),
    StructField("Affiliations",   StringType(), True),
    StructField("NOC",            StringType(), True),
    StructField("Athlete_Id",     IntegerType(), False),
])

RESULTS_SCHEMA = StructType([
    StructField("Games",        StringType(),  True),
    StructField("Event",        StringType(),  True),
    StructField("Team",         StringType(),  True),
    StructField("Pos",          StringType(),  True),
    StructField("Medal",        StringType(),  True),
    StructField("As",           StringType(),  True),
    StructField("NOC",          StringType(),  True),
    StructField("Discipline",   StringType(),  True),
    StructField("Nationality",  StringType(),  True),
    StructField("Athlete_Id",   IntegerType(), False),
])


# ----------------------------
# Main pipeline
# ----------------------------
def main():
    # Build IDs and filter out already scraped
    all_ids = list(range(1, SIZE))
    scraped = read_scraped_ids(spark)
    remaining_ids = [i for i in all_ids if i not in scraped]
    print(f"Total IDs: {len(all_ids)}, already scraped: {len(scraped)}, remaining: {len(remaining_ids)}")

    if not remaining_ids:
        print("Nothing to do.")
        job.commit()
        return

    # Create RDD with desired parallelism
    rdd = spark.sparkContext.parallelize(remaining_ids, NUM_PARTITIONS)
    # MapPartitions to perform scraping
    scraped_rows = rdd.mapPartitions(scrape_partition)

    # We will separate biodata, results, and errors
    # Convert to DataFrames
    # Helper to filter and convert
    biodata_rdd = scraped_rows.filter(lambda t: t[0] == "biodata").map(lambda t: t[1])
    results_rdd = scraped_rows.filter(lambda t: t[0] == "results").map(lambda t: t[1])
    errors_rdd = scraped_rows.filter(lambda t: t[0] == "error").map(lambda t: t[1])

    # If there are zero rows, attempt to create empty DF with schema
    # We'll infer schema by taking a small sample to create DataFrame safely
    
    # biodata_df = spark.createDataFrame(biodata_rdd, schema=BIODATA_SCHEMA) if not biodata_rdd.isEmpty() else None
    # results_df = spark.createDataFrame(results_rdd,schema= RESULTS_SCHEMA) if not results_rdd.isEmpty() else None
    
    

    # Safe DataFrame creation that preserves non-nullable integers
    def to_df_safe(rdd, schema):
        if rdd.isEmpty():
            return None
        return rdd.map(lambda d: Row(**d)).toDF(schema)

    biodata_df = to_df_safe(biodata_rdd, BIODATA_SCHEMA)
    results_df = to_df_safe(results_rdd, RESULTS_SCHEMA)

    errors_df = spark.createDataFrame(errors_rdd) if not errors_rdd.isEmpty() else None

    # Persist partition-level checkpoint: write partitioned outputs to checkpoints folder
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    if biodata_df is not None:
        check_bio_path = f"{CHECKPOINTS_PREFIX}/biodata_part_{ts}"
        biodata_df.write.mode("append").parquet(check_bio_path)
        print(f"Wrote biodata checkpoint to {check_bio_path}")

    if results_df is not None:
        check_res_path = f"{CHECKPOINTS_PREFIX}/results_part_{ts}"
        results_df.write.mode("append").parquet(check_res_path)
        print(f"Wrote results checkpoint to {check_res_path}")

    if errors_df is not None:
        errors_path = f"{BRONZE_BUCKET}/scrape_failures/failed_athletes_{ts}.parquet"
        errors_df.write.mode("append").parquet(errors_path)
        print(f"Wrote errors to {errors_path}")

    # Final aggregation: read all checkpoint files and write to raw_data paths (or append)
    # For simplicity, we'll read all checkpoint files that exist and write consolidated outputs
    try:
        all_bio_df = spark.read.parquet(f"{CHECKPOINTS_PREFIX}/biodata_part_*")
        # normalize strings: cast all columns except Athlete_Id to string
        for c in all_bio_df.columns:
            if c != "Athlete_Id":
                all_bio_df = all_bio_df.withColumn(c, all_bio_df[c].cast(StringType()))
        # coalesce to a few files to avoid tiny files
        all_bio_df.coalesce(4).write.mode("overwrite").parquet(RAW_BIODATA_PATH)
        print(f"Wrote final biodata to {RAW_BIODATA_PATH}")
    except Exception as e:
        print("No biodata to write or error:", e)

    try:
        all_res_df = spark.read.parquet(f"{CHECKPOINTS_PREFIX}/results_part_*")
        for c in all_res_df.columns:
            if c != "Athlete_Id":
                all_res_df = all_res_df.withColumn(c, all_res_df[c].cast(StringType()))
        all_res_df.coalesce(8).write.mode("overwrite").parquet(RAW_RESULTS_PATH)
        print(f"Wrote final results to {RAW_RESULTS_PATH}")
    except Exception as e:
        print("No results to write or error:", e)

    job.commit()

if __name__ == "__main__":
    main()