import sys
import requests
import pandas as pd
import random, time, re
from io import StringIO
from bs4 import BeautifulSoup

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


# -------------------------------------------------
# Helper function: Extract tables from Olympedia
# -------------------------------------------------
retry_num = 3

def game_table_df(session):
    """
    Scrape all Olympic Games edition tables from Olympedia
    and return combined Pandas DataFrame.
    """

    time.sleep(random.uniform(0.5, 1.5))
    url = "https://www.olympedia.org/editions"

    # Retry request
    for attempt in range(retry_num):
        try:
            response = session.get(url, timeout=60)
            if response.status_code == 200:
                break
        except requests.RequestException:
            if attempt == retry_num - 1:
                raise
            time.sleep(2)

    if response.status_code != 200:
        raise ValueError(f"Failed with status {response.status_code}")

    soup = BeautifulSoup(response.content, "html.parser")

    tables = []
    current_h2 = None
    current_h3 = None

    for tag in soup.find_all(["h2", "h3", "table"]):
        if tag.name == "h2":
            current_h2 = tag.get_text(strip=True)

        elif tag.name == "h3":
            current_h3 = tag.get_text(strip=True)

        elif tag.name == "table":

            # Convert <img> flags → NOC codes
            for img in tag.find_all("img"):
                src = img.get("src", "")
                m = re.search(r"/([A-Z]{3})\.png", src)
                img.replace_with(m.group(1) if m else "")

            df = pd.read_html(StringIO(str(tag)))[0]
            df["Game_Type"] = current_h2
            df["Edition_Name"] = current_h3
            tables.append(df)

    return pd.concat(tables, ignore_index=True)


# -------------------------------------------------
# 1. Glue Boilerplate
# -------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("=== Glue Job: Scrape Olympedia Editions Started ===")

# -------------------------------------------------
# 2. Run Scraper (driver-side)
# -------------------------------------------------
print("Scraping Olympedia editions...")

with requests.Session() as session:
    session.headers.update({
        "User-Agent": "OlympediaScraper/1.0 (+your-email@gmail.com)"
    })
    df = game_table_df(session)

print(f"Scraped {len(df)} rows.")

# Convert pandas → Spark
spark_df = spark.createDataFrame(df)

print("=== Preview Spark DataFrame ===")
spark_df.show(10, truncate=False)

# -------------------------------------------------
# 3. Write to Bronze (LocalStack S3)
# -------------------------------------------------
bronze_path = "s3://bronze/raw_data/editions.parquet"

print(f"Writing editions → {bronze_path}")
spark_df.write.mode("overwrite").parquet(bronze_path)

# -------------------------------------------------
# 4. Confirm Write
# -------------------------------------------------
print("=== Confirming Bronze Parquet ===")
spark.read.parquet(bronze_path).show(5, truncate=False)


job.commit()
print("=== Glue Job Completed ===")
