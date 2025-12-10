import re
import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import split, trim, regexp_replace
from pyspark.sql.functions import regexp_extract, to_date
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window




def drop_invalid_columns(df, columns_to_drop):
    existing = [c for c in columns_to_drop if c in df.columns]
    return df.drop(*existing)


def name_parsing(df):
    return df.withColumn(
        "Name",
        F.regexp_replace(F.col("Used name"), "•", " ")
    )



def measurements_parsing(df):
    parts = split(F.col("Measurements"), "/")

    return (
        df
        .withColumn("Height (cm)",
                    F.regexp_replace(trim(parts.getItem(0)), " cm", "").cast("double"))
        .withColumn("Weight (kg)",
                    F.regexp_replace(trim(parts.getItem(1)), " kg", "").cast("double"))
    )




date_pattern = r"(\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4})"

def date_parsing(df):
    df = (
        df
        .withColumn("Born_Date_Str", regexp_extract("Born", date_pattern, 1))
        .withColumn("Died_Date_Str", regexp_extract("Died", date_pattern, 1))
    )
    
    df = df.withColumn(
        "Born_Date",
        to_date("Born_Date_Str", "dd MMMM yyyy")
    )

    df = df.withColumn(
        "Died_Date",
        to_date("Died_Date_Str", "dd MMMM yyyy")
    )

    df = df.withColumn("Is_Alive", F.col("Died_Date").isNull())

    return df.drop("Born_Date_Str", "Died_Date_Str")


def location_parsing(df):
    pattern = r"in\s+(.+?),\s*([A-Za-zÀ-ÿ'’\- ]+)\s*\((\w+)\)$"

    return (
        df
        .withColumn("Born_City",   F.regexp_extract("Born", pattern, 1))
        .withColumn("Born_Region", F.regexp_extract("Born", pattern, 2))
        .withColumn("Born_Country",F.regexp_extract("Born", pattern, 3))
    )





def roles_parsing(df):
    return (
        df.withColumn(
            "Roles",
            F.regexp_replace(
                F.trim(F.col("Roles").cast("string")),
                "—",
                ""
            )
        )
        .withColumn(
            "Roles",
            F.regexp_replace(F.col("Roles"), r" \u2022 ", ",")
        )
    )


# Normalize + explode affiliations

def explode_affiliations(df):
    return (
        df
        .withColumn("Affiliations", F.col("Affiliations").cast("string"))
        .filter(F.col("Affiliations").isNotNull())
        .withColumn("Affiliations", F.split(F.col("Affiliations"), r"\s*/\s*"))
        .withColumn("Affiliations", F.explode("Affiliations"))
    )

# Extract components Club, City (Country)

def extract_affiliation_components(df):
    pattern = r'^(.+?)(?:,\s*(.+?))?(?:\s*\((.+?)\))?$'

    return (
        df
        .withColumn("Affiliation_Club",   F.regexp_extract("Affiliations", pattern, 1))
        .withColumn("Affiliation_City",   F.regexp_extract("Affiliations", pattern, 2))
        .withColumn("Affiliation_Country",F.regexp_extract("Affiliations", pattern, 3))
    )

# Build dim_affiliation table

def build_dim_affiliation(df):
    dim = (
        df.select(
            "Affiliation_Club",
            "Affiliation_City",
            "Affiliation_Country"
        )
        .dropDuplicates()
        .withColumn("Affiliation_Id", F.monotonically_increasing_id())
    )
    return dim

# Build bridge table

def build_bridge(df, dim):
    bridge = (
        df.join(
            dim,
            on=["Affiliation_Club", "Affiliation_City", "Affiliation_Country"],
            how="left"
        )
        .select("Athlete_Id", "Affiliation_Id")
        .dropDuplicates()
    )
    return bridge


# Country code fallback cleanup

def normalize_city_country(dim):
    pattern = r'^\(([A-Za-z]{3})\)$'

    return (
        dim
        .withColumn(
            "City_Matches_Country",
            F.regexp_extract("Affiliation_City", pattern, 1)
        )
        .withColumn(
            "Affiliation_Country",
            F.when(F.col("City_Matches_Country") != "", F.col("City_Matches_Country"))
             .otherwise(F.col("Affiliation_Country"))
        )
        .withColumn(
            "Affiliation_City",
            F.when(F.col("City_Matches_Country") != "", None)
             .otherwise(F.col("Affiliation_City"))
        )
        .drop("City_Matches_Country")
    )

# Full affiliations parsing 

def affiliations_parsing(df, spark):
    # Step 1: explode
    exploded = explode_affiliations(df)

    # Step 2: extract components
    extracted = extract_affiliation_components(exploded)

    # Step 3: build dimension table
    dim_affiliation = build_dim_affiliation(extracted)
    dim_affiliation = normalize_city_country(dim_affiliation)

    # Step 4: build bridge table
    bridge_athlete_affiliation = build_bridge(extracted, dim_affiliation)

    return dim_affiliation, bridge_athlete_affiliation

# NOC parsing with country matching

# Legacy mapping (historical country names → current equivalents)
legacy_map = {
    "west germany": "germany",
    "east germany": "germany",
    "germany west germany": "germany",
    "germany saar": "germany",
    "german democratic republic": "germany",
    "saar": "germany",
    "soviet union": "russian federation",
    "ussr": "russian federation",
    "unified team": "russian federation",
    "czechoslovakia": "czechia",
    "bohemia": "czechia",
    "yugoslavia": "serbia",
    "serbia and montenegro": "serbia",
    "rhodesia": "zimbabwe",
    "malaya": "malaysia",
    "north yemen": "yemen",
    "south yemen": "yemen",
    "burma": "myanmar",
    "peoples republic of china": "china",
    "republic of korea": "south korea",
    "korea team": "south korea",
    "democratic people's republic of korea": "north korea",
    "islamic republic of iran": "iran",
    "kingdom of saudi arabia": "saudi arabia",
    "united arab republic": "egypt",
    "republic of moldova": "moldova",
    "roc": "russian federation",
    "great britain": "united kingdom",
    "the bahamas": "bahamas",
    "hong kong, china": "hong kong",
    "taiwan": "chinese taipei",
    "viet nam": "vietnam",
}


def extract_valid_nocs_udf(countries, legacy_map):
    def process(noc_str):
        if noc_str is None:
            return ""

        s = noc_str.lower().strip()

        # Apply legacy mappings
        for old, new in legacy_map.items():
            if old in s:
                s = s.replace(old, new)

        # Normalize separators
        s = re.sub(r"[/,;]", " ", s)
        s = re.sub(r"\band\b", " ", s)
        s = re.sub(r"\s+", " ", s).strip()

        found = set()

        # scan for country names
        for country in sorted(countries, key=len, reverse=True):
            pattern = rf'(?<!\w){re.escape(country)}(?!\w)'
            if re.search(pattern, s):
                found.add(country)
                s = re.sub(pattern, " ", s)

        if not found:
            return re.sub(r"\s+", " ", noc_str.lower().strip())

        return ",".join(sorted(found))

    return udf(process, StringType())


def noc_parsing(df, countries_df, spark):

    valid_countries = (
        countries_df
        .select(F.lower(F.col("English short name lower case")).alias("country"))
        .rdd.flatMap(lambda x: x)
        .collect()
    )

    bc_countries = spark.sparkContext.broadcast(valid_countries)
    bc_legacy = spark.sparkContext.broadcast(legacy_map)

    extract_udf = extract_valid_nocs_udf(bc_countries.value, bc_legacy.value)

    return df.withColumn("NOC", extract_udf(F.col("NOC")))


def clean_biodata(df, countries_df, spark):
    """
    Master cleaning pipeline for athlete biodata (Spark version).

    Returns:
        cleaned_df
        dim_affiliation
        bridge_affiliation
    """

    df = name_parsing(df)
    df = measurements_parsing(df)
    df = date_parsing(df)
    df = location_parsing(df)
    df = roles_parsing(df)

    # df = noc_parsing(df, countries_df, spark)
    dim_affiliation, bridge_affiliation = affiliations_parsing(df, spark)

    columns_to_drop = [
        'Used name', 'Born', 'Died', 'Full name', 'Measurements',
        'Affiliations', 'Title(s)', 'Nationality', 'Other names',
        'Original name', 'Name order', 'Nick/petnames'
    ]
    df = drop_invalid_columns(df, columns_to_drop)

    return df, dim_affiliation, bridge_affiliation


# # ---------------------------------------------------------------------
# # 1. Glue boilerplate
# # ---------------------------------------------------------------------
# spark.stop()
# args = getResolvedOptions(sys.argv, ["JOB_NAME"])
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark: SparkSession = glueContext.spark_session

# spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# job = Job(glueContext)
# job.init(args["JOB_NAME"], args)

# print("=== Glue Job Started ===")

# # ---------------------------------------------------------------------
# # 2. Load bronze inputs from LocalStack S3
# # ---------------------------------------------------------------------
# bronze_biodata_path = "s3://bronze/raw_data/biodata.parquet"
# bronze_countries_path = "s3://bronze/data/wikipedia-iso-country-codes.csv"
# bronze_results_path = "s3://bronze/olympic_raw/results.csv"


# print(f"Reading biodata from: {bronze_biodata_path}")
# print(f"Reading countries lookup from: {bronze_countries_path}")

# biodata_df = (
#     spark.read
#          .option("header", "true")
#          .option("inferSchema", "true")
#          .parquet(bronze_biodata_path)
# )

# countries_df = (
#     spark.read
#          .option("header", "true")
#          .option("inferSchema", "true")
#          .csv(bronze_countries_path)
# )


# print("Sample of bronze biodata:")
# biodata_df.show(5, truncate=False)

# # ---------------------------------------------------------------------
# # 3. Run cleaning pipeline
# # ---------------------------------------------------------------------
# print("=== Running clean_biodata() ===")

# cleaned_df, dim_affiliation, bridge_affiliation = clean_biodata(
#     biodata_df,
#     countries_df,
#     spark
# )

# print("=== Cleaned Biodata Preview ===")
# cleaned_df.show(10, truncate=False)

# print("=== dim_affiliation Preview ===")
# dim_affiliation.show(10, truncate=False)

# print("=== bridge_affiliation Preview ===")
# bridge_affiliation.show(10, truncate=False)

# # ---------------------------------------------------------------------
# # 4. Write to Silver bucket (LocalStack S3)
# # ---------------------------------------------------------------------
# silver_base = "s3://silver/olympic_clean/"

# cleaned_path = silver_base + "biodata/"
# aff_dim_path = silver_base + "dim_affiliation/"
# aff_bridge_path = silver_base + "bridge_affiliation/"

# print(f"Writing cleaned biodata → {cleaned_path}")
# cleaned_df.write.mode("overwrite").parquet(cleaned_path)

# print(f"Writing dim_affiliation → {aff_dim_path}")
# dim_affiliation.write.mode("overwrite").parquet(aff_dim_path)

# print(f"Writing bridge_affiliation → {aff_bridge_path}")
# bridge_affiliation.write.mode("overwrite").parquet(aff_bridge_path)

# # ---------------------------------------------------------------------
# # 5. Confirm writes & Finish Glue job
# # ---------------------------------------------------------------------
# print("=== Confirming written datasets ===")
# spark.read.parquet(cleaned_path).show(5, truncate=False)
# spark.read.parquet(aff_dim_path).show(5, truncate=False)
# spark.read.parquet(aff_bridge_path).show(5, truncate=False)

# job.commit()
# print("=== Glue Job Completed ===")



# ------------------------------------------------------------
# Games → Year + Type (Spark)
# ------------------------------------------------------------
def game_year_type_parsing(df):
    """
    Split the 'Games' column into 'Game Year' (int) and 'Game Type' (string).
    Example: "2020 Summer" → 2020, "Summer".
    """

    # Extract year + type using regex
    extracted = F.regexp_extract("Games", r"^(\d{4})\s+(.*)$", 1)
    type_part = F.regexp_extract("Games", r"^(\d{4})\s+(.*)$", 2)

    df = (
        df.withColumn("Game Year", extracted.cast("int"))
          .withColumn("Game Type", type_part)
    )

    return df


# ------------------------------------------------------------
# Position + Tied Parsing (Spark)
# ------------------------------------------------------------
def position_parsing(df):
    """
    Extracts:
    - Position: numeric part of the "Pos" column
    - Tied: boolean if "=" or tie indicator found
    """

    df = (
        df.withColumn("Position", F.regexp_extract("Pos", r"(\d+)", 1).cast("int"))
          .withColumn("Tied", F.col("Pos").rlike("=.*"))
    )

    # Ensure boolean type (Spark uses boolean not pandas nullable boolean)
    df = df.withColumn("Tied", F.col("Tied").cast("boolean"))

    return df


# ------------------------------------------------------------
# Master Cleaning Pipeline for Results
# ------------------------------------------------------------
def clean_results(df):
    """
    Full Spark cleaning flow for athlete results.
    Matches the pandas version.
    """

    df = game_year_type_parsing(df)
    df = position_parsing(df)

    columns_to_drop = ["Nationality", "Unnamed: 7", "Games", "Pos"]

    # Drop only columns that actually exist (avoids runtime errors)
    for c in columns_to_drop:
        if c in df.columns:
            df = df.drop(c)

    return df




# --------------------------------------------------------------------
# normalize_competition(text) — rewritten using Spark expressions
# --------------------------------------------------------------------
def normalize_competition_column(df):
    """
    Normalize 'Competition' text using regex and Spark functions.
    Handles:
        "6-13 April" → "6 April - 13 April"
    """
    # Replace long dash
    df = df.withColumn("Competition", F.regexp_replace("Competition", "–", "-"))

    # Detect cases like "6-13 April"
    pattern = r"^(\d+)\s*-\s*(\d+)\s*([A-Za-z]+)$"

    df = df.withColumn(
        "Competition",
        F.when(
            F.col("Competition").rlike(pattern),
            F.concat_ws(
                " ",
                F.regexp_extract("Competition", pattern, 1),          # start day
                F.regexp_extract("Competition", pattern, 3)           # month
            ).cast("string")
            + F.lit(" - ")
            + F.concat_ws(
                " ",
                F.regexp_extract("Competition", pattern, 2),          # end day
                F.regexp_extract("Competition", pattern, 3)           # month
            )
        ).otherwise(F.col("Competition"))
    )

    return df


# --------------------------------------------------------------------
# parse_competition(df)
# --------------------------------------------------------------------
def parse_competition(df):
    df = df.withColumn(
        "Competition_Start", F.split("Competition", "-").getItem(0).cast("string")
    ).withColumn(
        "Competition_End", F.split("Competition", "-").getItem(1).cast("string")
    )

    df = df.withColumn("Competition_Start", F.trim("Competition_Start"))
    df = df.withColumn("Competition_End", F.trim("Competition_End"))

    return df


# --------------------------------------------------------------------
# format_date(df, date_cols)
# --------------------------------------------------------------------
def format_date(df):
    date_cols = ["Opened", "Closed", "Competition_Start", "Competition_End"]

    for col in date_cols:
        df = df.withColumn(
            col,
            F.to_date(
                F.concat_ws(" ", F.col(col), F.col("Year").cast("string")),
                "d MMMM yyyy"
            )
        )

    return df


# --------------------------------------------------------------------
# remove_ancient_games(df)
# --------------------------------------------------------------------
def remove_ancient_games(df):
    return df.filter(F.col("Game_Type") != "Ancient Olympic Games")


# --------------------------------------------------------------------
# rename_comments_column(df)
# --------------------------------------------------------------------
def rename_comments_column(df):
    if "Unnamed: 7" in df.columns:
        return df.withColumnRenamed("Unnamed: 7", "Comments")
    return df


# --------------------------------------------------------------------
# adding_game_id_column(df)
# --------------------------------------------------------------------
def adding_game_id_column(df):
    window = Window.orderBy("Year", "Game_Type", "Edition_Name")
    df = df.withColumn("Game_Id", F.row_number().over(window))
    return df


# --------------------------------------------------------------------
# clean_editions(df)
# --------------------------------------------------------------------
def clean_editions(df):
    df = normalize_competition_column(df)
    df = parse_competition(df)
    df = format_date(df)
    df = remove_ancient_games(df)
    df = rename_comments_column(df)

    # Drop unused
    for c in ["#", "Competition"]:
        if c in df.columns:
            df = df.drop(c)

    df = adding_game_id_column(df)

    return df





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

print("=== Glue Job Started ===")

# ---------------------------------------------------------------------
# 2. Load bronze inputs from LocalStack S3
# ---------------------------------------------------------------------
bronze_biodata_path = "s3://bronze/raw_data/biodata.parquet"
bronze_results_path = "s3://bronze/raw_data/results.parquet"
bronze_editions_path = "s3://bronze/raw_data/editions.parquet"
bronze_countries_path = "s3://bronze/data/wikipedia-iso-country-codes.csv"



print("=== Reading Bronze Files ===")

biodata_df = spark.read.parquet(bronze_biodata_path)
results_df = spark.read.parquet(bronze_results_path)
editions_df = spark.read.parquet(bronze_editions_path)
countries_df = spark.read.option("header", "true").csv(bronze_countries_path)



# ---------------------------------------------------------------------
# 3. Run cleaning pipelines
# ---------------------------------------------------------------------
print("=== CLEANING BIODATA ===")
cleaned_biodata, dim_affiliation, bridge_affiliation = clean_biodata(
    biodata_df, countries_df, spark
)

print("=== CLEANING RESULTS ===")
cleaned_results = clean_results(results_df)

print("=== CLEANING EDITIONS ===")
cleaned_editions = clean_editions(editions_df)

# ---------------------------------------------------------------------
# 4. Write to Silver bucket (LocalStack S3)
# ---------------------------------------------------------------------
silver_base = "s3://silver/clean_data_I/"

paths = {
    "biodata": silver_base + "cleaned_biodata.parquet",
    "dim_affiliation": silver_base + "dim_affiliation.parquet",
    "bridge_affiliation": silver_base + "bridge_athlete_affiliation.parquet",
    "results": silver_base + "cleaned_results.parquet",
    "editions": silver_base + "cleaned_editions.parquet",
}

print("=== Writing Silver Outputs ===")

cleaned_biodata.write.mode("overwrite").parquet(paths["biodata"])
dim_affiliation.write.mode("overwrite").parquet(paths["dim_affiliation"])
bridge_affiliation.write.mode("overwrite").parquet(paths["bridge_affiliation"])
cleaned_results.write.mode("overwrite").parquet(paths["results"])
cleaned_editions.write.mode("overwrite").parquet(paths["editions"])

# ---------------------------------------------------------------------
# 5. Confirm writes & Finish Glue job
# ---------------------------------------------------------------------
print("=== Confirming outputs ===")
for name, path in paths.items():
    print(f"Preview → {path}")
    spark.read.parquet(path).show(5, truncate=False)

job.commit()
print("=== Glue Job Completed ===")
