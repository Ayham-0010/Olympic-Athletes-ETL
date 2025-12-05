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
