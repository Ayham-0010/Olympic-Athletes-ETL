import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark import SparkContext

# ---------------------------------------------------------------------
# Mapping dictionaries
# ---------------------------------------------------------------------

athlete_dim_table_columns_names = {
    "Athlete_Id": "athlete_id",
    "Name": "athlete_name",
    "Roles": "athlete_roles",
    "Sex": "athlete_sex",
    "NOC": "athlete_NOC",
    "Height (cm)": "athlete_height_cm",
    "Weight (kg)": "athlete_weight_kg",
    "Born_Date": "athlete_born_date",
    "Died_Date": "athlete_died_date",
    "Is_Alive": "athlete_is_alive",
    "Born_City": "athlete_born_city",
    "Born_Region": "athlete_born_region",
    "Born_Country": "athlete_born_country",
    "Height_Imputed": "athlete_is_height_imputed",
    "Weight_Imputed": "athlete_is_weight_imputed",
    "Born_Country_From_NOC": "athlete_is_born_country_from_NOC"
}

affiliation_dim_table_columns_names = {
    "Affiliation_Id": "affiliation_id",
    "Affiliation_Club": "dim_affiliation_club",
    "Affiliation_City": "dim_affiliation_city",
    "Affiliation_Country": "dim_affiliation_country"
}

athlete_affiliation_bridge_table_columns_names = {
    "Athlete_Id": "athlete_id",
    "Affiliation_Id": "affiliation_id"
}

games_dim_table_columns_names = {
    "Game_Id": "game_id",
    "Game_Type": "dim_game_type",
    "Edition_Name": "dim_edition_name",
    "Year": "dim_game_year",
    "City": "dim_city",
    "Country": "dim_country",
    "Opened": "dim_opened",
    "Closed": "dim_closed",
    "Competition_Start": "dim_competition_start",
    "Competition_End": "dim_competition_end",
    "Comments": "dim_comments",
    "Opened_Imputed": "dim_opened_imputed",
    "Closed_Imputed": "dim_closed_imputed",
    "Competition_Start_Imputed": "dim_competition_start_imputed",
    "Competition_End_Imputed": "dim_competition_end_imputed"
}

events_fct_table_columns_names = {
    "Athlete_Id": "athlete_id",
    "NOC": "dim_noc",
    "Discipline": "dim_discipline",
    "Game Type": "dim_game_type",
    "Game Year": "dim_game_year",
    "Event": "dim_event_name",
    "Team": "dim_team_name",
    "As": "dim_as",
    "Tied": "m_tied_flag",
    "Position": "m_position",
    "Medal": "m_medal"
}

# ---------------------------------------------------------------------
# Spark renaming + ordering function
# ---------------------------------------------------------------------

def rename_and_reorder_spark(df, mapping):
    # Rename columns
    for old, new in mapping.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    # Reorder columns
    ordered_cols = [new for old, new in mapping.items() if new in df.columns]

    return df.select(*ordered_cols)

# ---------------------------------------------------------------------
# INIT GLUE JOB
# ---------------------------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
print("=== Glue Job Started: column_rename_reorder ===")

# ---------------------------------------------------------------------
# READ SILVER INPUTS
# ---------------------------------------------------------------------

silver = "s3://silver"
gold = "s3://gold/clean_data_final"

paths = {
    "bios": f"{silver}/clean_data_II/imputed_biodata.parquet",
    "results": f"{silver}/clean_data_I/cleaned_results.parquet",
    "games": f"{silver}/clean_data_II/imputed_editions.parquet",
    "aff": f"{silver}/clean_data_I/dim_affiliation.parquet",
    "bridge": f"{silver}/clean_data_I/bridge_athlete_affiliation.parquet",
}

bios_df = spark.read.parquet(paths["bios"])
results_df = spark.read.parquet(paths["results"])
games_df = spark.read.parquet(paths["games"])
aff_df = spark.read.parquet(paths["aff"])
bridge_df = spark.read.parquet(paths["bridge"])

# ---------------------------------------------------------------------
# APPLY RENAME & REORDER
# ---------------------------------------------------------------------

dim_athletes_df = rename_and_reorder_spark(bios_df, athlete_dim_table_columns_names)
dim_affiliations_df = rename_and_reorder_spark(aff_df, affiliation_dim_table_columns_names)
bridge_ath_aff_df = rename_and_reorder_spark(bridge_df, athlete_affiliation_bridge_table_columns_names)
dim_games_df = rename_and_reorder_spark(games_df, games_dim_table_columns_names)
fct_results_df = rename_and_reorder_spark(results_df, events_fct_table_columns_names)

# ---------------------------------------------------------------------
# WRITE GOLD OUTPUT — AS SINGLE FILE
# ---------------------------------------------------------------------

def write_single(df, path):
    df.coalesce(1).write.mode("overwrite").parquet(path)

write_single(dim_athletes_df, f"{gold}/dim_athletes.parquet")
write_single(dim_affiliations_df, f"{gold}/dim_affiliations.parquet")
write_single(bridge_ath_aff_df, f"{gold}/bridge_athletes_affiliations.parquet")
write_single(dim_games_df, f"{gold}/dim_games.parquet")
write_single(fct_results_df, f"{gold}/fct_results.parquet")

# ---------------------------------------------------------------------
# CONFIRMATION
# ---------------------------------------------------------------------
print("=== Sample Outputs ===")
dim_athletes_df.show(5, truncate=False)
dim_games_df.show(5, truncate=False)
fct_results_df.show(5, truncate=False)

job.commit()
print("=== Glue Job Completed Successfully ===")
