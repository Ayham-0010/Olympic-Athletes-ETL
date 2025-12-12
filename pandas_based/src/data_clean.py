import pandas as pd
import numpy as np
import boto3
import s3fs
import re, os

from .logger import logger





def drop_invalid_columns(df, columns_to_drop):

    """
    Drop unwanted or redundant columns from a DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.
        columns_to_drop (list): List of column names to remove.

    Returns:
        pd.DataFrame: Cleaned DataFrame with invalid columns dropped.
    """

    df = df.copy()

    df = df.drop(columns=columns_to_drop, errors='ignore')

    return df


# BIODATA CLEANING FUNCTIONS


def name_parsing(df):
    """
    Standardize athlete names by replacing special characters.

    Specifically replaces '•' with a space to improve readability.
    """

    df = df.copy()

    df["Name"]= df["Used name"].str.replace('•',' ', regex=False)
    return df

def measurements_parsing(df):
    """
    Split 'Measurements' column into numeric Height (cm) and Weight (kg).

    Handles cases like '180 cm / 75 kg'.
    """

    df = df.copy()
    df['Height (cm)'] = pd.to_numeric(df['Measurements'].str.split('/').str[0].str.strip(' cm'), errors='coerce')
    df['Weight (kg)'] =pd.to_numeric(df['Measurements'].str.split('/').str[1].str.strip(' kg'), errors='coerce')

    return df

def date_parsing(df):
    """
    Extract and parse birth and death dates from text.

    Adds:
        - Born_Date, Died_Date: Parsed datetime columns
        - Is_Alive: Boolean flag
    """

    df = df.copy()

    date_pattern = r'(\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4})'

    df['Born_Date'] = df['Born'].str.extract(date_pattern)
    df['Born_Date'] = pd.to_datetime(df['Born_Date'], format="mixed", errors='coerce')

    df['Died_Date'] = df['Died'].str.extract(date_pattern)
    df['Died_Date'] = pd.to_datetime(df['Died_Date'], format="mixed", errors='coerce')

    df['Is_Alive'] = df['Died_Date'].isna()
    
    return df

def location_parsing(df):
    """
    Extract birthplace information into city, region, and country columns.
    """

    df = df.copy()

    location_pattern = r"in\s+(.+?),\s*([A-Za-zÀ-ÿ'’\- ]+)\s*\((\w+)\)$"

    df[['Born_City', 'Born_Region', 'Born_Country']] = df['Born'].str.extract(location_pattern, expand=True)
    return df


def affiliations_parsing(df):
    """
    Normalize and decompose athlete affiliations into dimensional and bridge tables.

    Produces:
        - dim_affiliation: Unique list of affiliations (club, city, country)
        - bridge_athlete_affiliation: Mapping between athletes and affiliations
    """

    df = df.copy()
    df = df.assign(Affiliations=df['Affiliations'].astype(str))
    df = df.dropna(subset=['Affiliations'])
    df = df.assign(Affiliations=df['Affiliations'].str.split(r'\s*/\s*'))
    df = df.explode('Affiliations').reset_index(drop=True)

    # Extract components (Club, City, Country)
    extracted = df['Affiliations'].str.extract(r'^(.+?)(?:,\s*(.+?))?(?:\s*\((.+?)\))?$')
    extracted.columns = ['Affiliation_Club', 'Affiliation_City', 'Affiliation_Country']

    df = pd.concat([df[['Athlete_Id']], extracted], axis=1)

    # Dimension table: unique affiliations
    dim_affiliation = (
        df[['Affiliation_Club', 'Affiliation_City', 'Affiliation_Country']]
        .drop_duplicates()
        .reset_index(drop=True)
        .reset_index(names='Affiliation_Id')
    )

    # Bridge table: athlete - affiliation mapping
    bridge_athlete_affiliation = (
        df.merge(
            dim_affiliation,
            on=['Affiliation_Club', 'Affiliation_City', 'Affiliation_Country'],
            how='left'
        )[["Athlete_Id", "Affiliation_Id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )


    # Pattern to match a string like "(xxx)"
    pattern = r'^\(([A-Za-z]{3})\)$'

    mask = dim_affiliation['Affiliation_City'].str.match(pattern, na=False)

    dim_affiliation.loc[mask, 'Affiliation_Country'] = (
        dim_affiliation.loc[mask, 'Affiliation_City']
        .str.extract(pattern)[0]
    )

    dim_affiliation.loc[mask, 'Affiliation_City'] = np.nan
    
    return dim_affiliation, bridge_athlete_affiliation


def roles_parsing(df):
    """
    Clean and standardize the 'Roles' column by removing unwanted symbols
    and replacing bullet separators with commas.
    """ 

    df = df.copy()
    df['Roles'] = (
        df['Roles']
        .astype(str)
        .str.strip()
        .str.replace('—', '')
        .str.replace(' • ', ',', regex=False)  
    )
    return df




def noc_parsing(df, countries_df):
    """
    Normalize National Olympic Committee (NOC) or country information.

    - Maps legacy/historical names to modern equivalents.
    - Ensures only valid ISO country names are retained.
    """

    df = df.copy()
  
    # Normalize valid country names
    valid_countries = countries_df["English short name lower case"].str.lower().str.strip().tolist()

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

    def extract_valid_nocs(noc_str):
        if pd.isna(noc_str):
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
     
        for country in sorted(valid_countries, key=len, reverse=True):
            pattern = rf'(?<!\w){re.escape(country)}(?!\w)'
            if re.search(pattern, s):
                found.add(country)
                s = re.sub(pattern, " ", s)

        # Return original cleaned string if no matches
        if not found:
            cleaned_original = re.sub(r"\s+", " ", noc_str.lower().strip())
            return cleaned_original

        result = ",".join(sorted(found))
        return result


    df["NOC"] = df["NOC"].apply(extract_valid_nocs)

    return df


def clean_biodata(df,countries_df):
    """
    Master cleaning pipeline for athlete biodata.

    Applies all individual cleaning steps and returns:
        - Cleaned biodata
        - dim_affiliation (dimension table)
        - bridge_athlete_affiliation (bridge table)
    """

    df = name_parsing(df)
    df = measurements_parsing(df)
    df = date_parsing(df)
    df = location_parsing(df)
    df = roles_parsing(df)
    df = noc_parsing(df,countries_df)

    dim_affiliation, bridge_athlete_affiliation = affiliations_parsing(df)

    columns_to_drop = [ 'Used name', 'Born', 'Died', 'Full name', 'Measurements', 'Affiliations','Title(s)',  'Nationality', 'Other names', 'Original name', 'Name order', 'Nick/petnames']
    df = drop_invalid_columns(df, columns_to_drop)
    return df, dim_affiliation, bridge_athlete_affiliation



# RESULTS CLEANING FUNCTIONS


def game_year_type_parsing(df):
    """
    Split the 'Games' column into year and type (e.g., "2020 Summer").
    """

    df = df.copy()

    df[['Game Year', 'Game Type']] = df['Games'].str.extract(r'^(\d{4})\s+(.*)$', expand=True)
    df['Game Year'] = pd.to_numeric(df['Game Year'])

    return df

def position_parsing(df):
    """
    Parse and standardize athlete positions.

    Extracts numeric position, detects ties (e.g., "1=", "2T"), and stores as boolean.
    """

    df = df.copy()

    df["Position"] = df["Pos"].str.extract(r"(\d+)")
    df["Position"] = pd.to_numeric(df["Position"], errors="coerce")

    df["Tied"] = df["Pos"].astype(str).str.contains("=", na=False)

    df["Tied"] = (
        df["Tied"]
        .replace(["True", "true", "TRUE"], True)
        .replace(["False", "false", "FALSE"], False)
        .astype("boolean")
    )

    return df

def clean_results(df):
    """
    Apply all cleaning steps to athlete results dataset.
    """

    df = game_year_type_parsing(df)
    df = position_parsing(df)

    columns_to_drop = ['Nationality', 'Unnamed: 7', 'Games', 'Pos']
    df = drop_invalid_columns(df, columns_to_drop)

    return df



# EDITIONS CLEANING FUNCTIONS

def normalize_competition(text):
    """
    Normalize the 'Competition' text field to ensure consistent date formatting.
    """

    if pd.isna(text):
        return text

    text = text.replace("–", "-")
    
    # Fix cases like "6-13 April" → "6 April - 13 April"
    match = re.match(r"(\d+)\s*-\s*(\d+\s+[A-Za-z]+)", text)
    if match:
        day_start, day_end_part = match.groups()
        month = re.search(r"[A-Za-z]+", day_end_part).group(0)
        return f"{day_start} {month} - {day_end_part}"
    return text


def parse_competition(df):
    """
    Split 'Competition' column into start and end date segments.
    """

    df = df.copy()
    df[["Competition_Start", "Competition_End"]] = (
        df["Competition"]
        .str.split("-", n=1, expand=True)  
        .apply(lambda x: x.str.strip())
    )
    return df



def format_date(df, date_cols):
    """
    Convert multiple date columns into standardized 'YYYY-MM-DD' format.
    """

    df = df.copy()
    for col in date_cols:
        df[col] = pd.to_datetime(
            df[col] + " " + df["Year"].astype(str),
            format="%d %B %Y",
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")
    return df
    
def remove_ancient_games(df):
    """
    Remove entries corresponding to Ancient Olympic Games.
    """

    df = df.copy()
    df = df[df['Game_Type'] != 'Ancient Olympic Games']
    return df

def rename_comments_column(df):
    """
    Rename unnamed comments column to 'Comments' for clarity.
    """

    df = df.copy()
    df = df.rename(columns={'Unnamed: 7':'Comments'})
    return df

def adding_game_id_column(df):
    """
    Add sequential surrogate key 'Game_Id' for the editions dataset.
    """

    df = df.copy()
    df = df.sort_values(
        by=["Year", "Game_Type", "Edition_Name"],
        ascending=[True, True, True]
    ).reset_index(drop=True)

    df["Game_Id"] = range(1, len(df) + 1)
    return df



def clean_editions(df):
    """
    Full cleaning pipeline for Olympic editions data.
    """

    df = df.copy()

    df["Competition"] = df["Competition"].apply(normalize_competition)
    df = parse_competition(df)

    date_cols = ["Opened", "Closed", "Competition_Start", "Competition_End"]
    df = format_date(df, date_cols)
    df = remove_ancient_games(df)
    df = rename_comments_column(df)

    columns_to_drop = ['#', 'Competition']
    df = drop_invalid_columns(df, columns_to_drop)
    df =  adding_game_id_column(df)

    return df


# MAIN CLEANING PIPELINE (Stage I)

def data_clean_I():
    """
    Stage I cleaning pipeline.

    Reads raw Parquet and CSV data from the bronze bucket,
    cleans each dataset, and writes cleaned Parquet files
    to the silver bucket (MinIO/S3).
    """

    # S3/MinIO configuration
    s3_endpoint = "http://minio:9000"  
    access_key = "accesskey"
    secret_key = "secretkey"
    bronze_bucket = "bronze"
    silver_bucket = "silver"

    # Create s3fs-compatible path
    s3fs_opts = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": s3_endpoint},
    }

    # Load raw data from bronze layer
    logger.info("Reading raw data from bronze bucket...")
    bios_df = pd.read_parquet(f"s3://{bronze_bucket}/raw_data/biodata.parquet", storage_options=s3fs_opts)
    results_df = pd.read_parquet(f"s3://{bronze_bucket}/raw_data/results.parquet", storage_options=s3fs_opts)
    editions_df = pd.read_parquet(f"s3://{bronze_bucket}/raw_data/editions.parquet", storage_options=s3fs_opts)
    countries_df = pd.read_csv(f"s3://{bronze_bucket}/data/wikipedia-iso-country-codes.csv", storage_options=s3fs_opts)


    # Apply data cleaning transformations
    logger.info("Cleaning bios data...")
    bios_df, dim_affiliation_df, bridge_athlete_affiliation_df = clean_biodata(bios_df, countries_df)
    
    logger.info("Cleaning results data...")
    results_df = clean_results(results_df)
    
    logger.info("Cleaning editions data ...")
    editions_df = clean_editions(editions_df)

    # Write cleaned datasets to silver layer
    logger.info("Saving cleaned data to silver bucket as Parquet...")
    bios_df.to_parquet(f"s3://{silver_bucket}/clean_data/cleaned_biodata.parquet", index=False, storage_options=s3fs_opts)
    dim_affiliation_df.to_parquet(f"s3://{silver_bucket}/clean_data/dim_affiliation.parquet", index=False, storage_options=s3fs_opts)
    bridge_athlete_affiliation_df.to_parquet(f"s3://{silver_bucket}/clean_data/bridge_athlete_affiliation.parquet", index=False, storage_options=s3fs_opts)
    results_df.to_parquet(f"s3://{silver_bucket}/clean_data/cleaned_results.parquet", index=False, storage_options=s3fs_opts)
    editions_df.to_parquet(f"s3://{silver_bucket}/clean_data/cleaned_editions.parquet", index=False, storage_options=s3fs_opts)

    logger.info("Data cleaning stage I completed successfully.")
