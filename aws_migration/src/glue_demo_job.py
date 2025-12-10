# src/glue_demo_job.py
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# -------------------------------------------------
# 1. Glue boiler-plate
# -------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------------------------
# 2. Hard-coded demo data
# -------------------------------------------------
data = [
    ("Alice", 25, "Swimming"),
    ("Bob",   31, "Athletics"),
    ("Carol", 28, "Gymnastics"),
]

columns = ["name", "age", "discipline"]
df = spark.createDataFrame(data, columns)

# -------------------------------------------------
# 3. Write to Bronze bucket (LocalStack S3)
# -------------------------------------------------
bronze_path = "s3://bronze/olympic_demo/athletes.parquet"
df.write.mode("overwrite").parquet(bronze_path)
print(f"Written demo data to {bronze_path}")

# -------------------------------------------------
# 4. Read back and show
# -------------------------------------------------
df_read = spark.read.parquet(bronze_path)
df_read.show(truncate=False)

# -------------------------------------------------
# 5. Finish Glue job
# -------------------------------------------------
job.commit()




# ---------------------------------------------------------------------
# 1. Glue boilerplate
# ---------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print("=== Glue Job Started ===")

# ---------------------------------------------------------------------
# 2. Load bronze inputs from LocalStack S3
# ---------------------------------------------------------------------
bronze_biodata_path = "s3://bronze/olympic_raw/athlete_biodata.csv"
bronze_countries_path = "s3://bronze/data/wikipedia-iso-country-codes.csv"

print(f"Reading biodata from: {bronze_biodata_path}")
print(f"Reading countries lookup from: {bronze_countries_path}")

biodata_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(bronze_biodata_path)
)

countries_df = (
    spark.read
         .option("header", "true")
         .option("inferSchema", "true")
         .csv(bronze_countries_path)
)

print("Sample of bronze biodata:")
biodata_df.show(5, truncate=False)

# ---------------------------------------------------------------------
# 3. Run cleaning pipeline
# ---------------------------------------------------------------------
print("=== Running clean_biodata() ===")

cleaned_df, dim_affiliation, bridge_affiliation = clean_biodata(
    biodata_df,
    countries_df,
    spark
)

print("=== Cleaned Biodata Preview ===")
cleaned_df.show(10, truncate=False)

print("=== dim_affiliation Preview ===")
dim_affiliation.show(10, truncate=False)

print("=== bridge_affiliation Preview ===")
bridge_affiliation.show(10, truncate=False)

# ---------------------------------------------------------------------
# 4. Write to Silver bucket (LocalStack S3)
# ---------------------------------------------------------------------
silver_base = "s3://silver/olympic_clean/"

cleaned_path = silver_base + "biodata/"
aff_dim_path = silver_base + "dim_affiliation/"
aff_bridge_path = silver_base + "bridge_affiliation/"

print(f"Writing cleaned biodata → {cleaned_path}")
cleaned_df.write.mode("overwrite").parquet(cleaned_path)

print(f"Writing dim_affiliation → {aff_dim_path}")
dim_affiliation.write.mode("overwrite").parquet(aff_dim_path)

print(f"Writing bridge_affiliation → {aff_bridge_path}")
bridge_affiliation.write.mode("overwrite").parquet(aff_bridge_path)

# ---------------------------------------------------------------------
# 5. Confirm writes & Finish Glue job
# ---------------------------------------------------------------------
print("=== Confirming written datasets ===")
spark.read.parquet(cleaned_path).show(5, truncate=False)
spark.read.parquet(aff_dim_path).show(5, truncate=False)
spark.read.parquet(aff_bridge_path).show(5, truncate=False)

job.commit()
print("=== Glue Job Completed ===")