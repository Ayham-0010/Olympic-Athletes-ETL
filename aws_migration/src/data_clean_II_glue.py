import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------
# 1. Glue boilerplate
# ---------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

job = Job(glueContext)
job.init(args["JOB_NAME"], args)
print("=== Glue Job Started: Data Clean II ===")

# ---------------------------------------------------------------------
# 2. Load SILVER cleaned datasets
# ---------------------------------------------------------------------
silver_base = "s3://silver/clean_data_I/"
bronze_base = "s3://bronze/data/"

bios_path = silver_base + "cleaned_biodata.parquet"
results_path = silver_base + "cleaned_results.parquet"
editions_path = silver_base + "cleaned_editions.parquet"
iso_path = bronze_base + "wikipedia-iso-country-codes.csv"

print(f"Reading bios: {bios_path}")
bios_df = spark.read.parquet(bios_path)

print(f"Reading results: {results_path}")
results_df = spark.read.parquet(results_path)

print(f"Reading editions: {editions_path}")
editions_df = spark.read.parquet(editions_path)

print(f"Reading ISO codes: {iso_path}")
iso_df = (
    spark.read.option("header", "true")
              .option("inferSchema", "true")
              .csv(iso_path)
)

# =====================================================================
# 3A. HEIGHT & WEIGHT IMPUTATION BY DISCIPLINE
# =====================================================================

# 1. Get most frequent discipline per athlete
w_discipline = Window.partitionBy("Athlete_Id")
discipline_mode = (
    results_df
        .groupBy("Athlete_Id", "Discipline")
        .count()
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("Athlete_Id").orderBy(F.desc("count"))
        ))
        .filter("rn = 1")
        .select("Athlete_Id", "Discipline")
)

bios = bios_df.join(discipline_mode, on="Athlete_Id", how="left")

# 2. Flags before imputation
bios = (bios
    .withColumn("Height_Imputed", F.col("Height (cm)").isNull())
    .withColumn("Weight_Imputed", F.col("Weight (kg)").isNull())
)

# 3. Impute by (Sex, Discipline) median
w = Window.partitionBy("Sex", "Discipline")

bios = (bios
    .withColumn("height_med", F.percentile_approx("Height (cm)", 0.5).over(w))
    .withColumn("weight_med", F.percentile_approx("Weight (kg)", 0.5).over(w))
    .withColumn("Height (cm)", F.coalesce("Height (cm)", "height_med"))
    .withColumn("Weight (kg)", F.coalesce("Weight (kg)", "weight_med"))
)

# 4. Fallback: median by Sex only
w_sex = Window.partitionBy("Sex")
bios = (bios
    .withColumn("height_med_sex", F.percentile_approx("Height (cm)", 0.5).over(w_sex))
    .withColumn("weight_med_sex", F.percentile_approx("Weight (kg)", 0.5).over(w_sex))
    .withColumn("Height (cm)", F.coalesce("Height (cm)", "height_med_sex"))
    .withColumn("Weight (kg)", F.coalesce("Weight (kg)", "weight_med_sex"))
)

bios = bios.drop("height_med", "weight_med", "height_med_sex", "weight_med_sex", "Discipline")

# =====================================================================
# 3B. IMPUTE Born_Country using NOC + ISO
# =====================================================================

iso_clean = (
    iso_df
        .select(
            F.lower(F.col("English short name lower case")).alias("country_name"),
            F.col("Alpha-3 code").alias("Alpha3")
        )
)

first_noc = F.split(F.col("NOC"), ",").getItem(0)

bios = bios.withColumn("NOC_key", F.lower(first_noc))

bios = bios.join(
    iso_clean,
    bios.NOC_key == iso_clean.country_name,
    how="left"
)

bios = bios.withColumn(
    "Born_Country_From_NOC",
    F.when(F.col("Born_Country").isNull(), True).otherwise(False)
)

bios = bios.withColumn(
    "Born_Country",
    F.coalesce("Born_Country", "Alpha3")
)

bios = bios.drop("NOC_key", "country_name", "Alpha3")

# =====================================================================
# 3C. IMPUTE Opened / Closed using Competition dates
# =====================================================================

editions = editions_df

editions = editions.withColumn(
    "Opened_Imputed",
    F.when(F.col("Opened").isNull() & F.col("Competition_Start").isNotNull(), True).otherwise(False)
)

editions = editions.withColumn(
    "Closed_Imputed",
    F.when(F.col("Closed").isNull() & F.col("Competition_End").isNotNull(), True).otherwise(False)
)

editions = editions.withColumn(
    "Opened",
    F.when(F.col("Opened").isNull(), F.col("Competition_Start")).otherwise(F.col("Opened"))
)

editions = editions.withColumn(
    "Closed",
    F.when(F.col("Closed").isNull(), F.col("Competition_End")).otherwise(F.col("Closed"))
)

# =====================================================================
# 4. WRITE TO SILVER (clean_data_II)
# =====================================================================
silver_clean2 = "s3://silver/clean_data_II/"

bios_out = silver_clean2 + "imputed_biodata.parquet"
editions_out = silver_clean2 + "imputed_editions.parquet"

print(f"Writing bios → {bios_out}")
bios.write.mode("overwrite").parquet(bios_out)

print(f"Writing editions → {editions_out}")
editions.write.mode("overwrite").parquet(editions_out)

# =====================================================================
# 5. Confirm writes & finish job
# =====================================================================
print("=== Preview Updated Bios ===")
spark.read.parquet(bios_out).show(5, truncate=False)

print("=== Preview Updated Editions ===")
spark.read.parquet(editions_out).show(5, truncate=False)

job.commit()
print("=== Glue Job Completed: Data Clean II ===")
