import pandas as pd
import numpy as np
import boto3
import s3fs
import re, os

from .logger import logger



def drop_invalid_columns(df, columns_to_drop):

    df = df.copy()

    df = df.drop(columns=columns_to_drop, errors='ignore')

    return df


# Bio data cleaning


def name_parsing(df):

    df = df.copy()

    df["Name"]= df["Used name"].str.replace('•',' ', regex=False)
    return df

def measurements_parsing(df):
    df = df.copy()
    df['Height (cm)'] = pd.to_numeric(df['Measurements'].str.split('/').str[0].str.strip(' cm'), errors='coerce')
    df['Weight (kg)'] =pd.to_numeric(df['Measurements'].str.split('/').str[1].str.strip(' kg'), errors='coerce')

    return df

def date_parsing(df):

    df = df.copy()

    date_pattern = r'(\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4})'

    df['Born_Date'] = df['Born'].str.extract(date_pattern)
    df['Born_Date'] = pd.to_datetime(df['Born_Date'], format="mixed", errors='coerce')

    df['Died_Date'] = df['Died'].str.extract(date_pattern)
    df['Died_Date'] = pd.to_datetime(df['Died_Date'], format="mixed", errors='coerce')

    df['Is_Alive'] = df['Died_Date'].isna()
    
    return df

def location_parsing(df):

    df = df.copy()

    location_pattern = r"in\s+(.+?),\s*([A-Za-zÀ-ÿ'’\- ]+)\s*\((\w+)\)$"

    df[['Born_City', 'Born_Region', 'Born_Country']] = df['Born'].str.extract(location_pattern, expand=True)
    return df


def affiliations_parsing(df):

    df = df.copy()

    # Split multiple affiliations into separate rows
    df = df.assign(Affiliations=df['Affiliations'].astype(str))
    df = df.dropna(subset=['Affiliations'])
    df = df.assign(Affiliations=df['Affiliations'].str.split(r'\s*/\s*'))
    df = df.explode('Affiliations').reset_index(drop=True)

    # Extract components: Club, City, Country
    extracted = df['Affiliations'].str.extract(r'^(.+?)(?:,\s*(.+?))?(?:\s*\((.+?)\))?$')
    extracted.columns = ['Affiliation_Club', 'Affiliation_City', 'Affiliation_Country']

    df = pd.concat([df[['Athlete_Id']], extracted], axis=1)

    # Drop duplicates to get unique affiliations
    dim_affiliation = (
        df[['Affiliation_Club', 'Affiliation_City', 'Affiliation_Country']]
        .drop_duplicates()
        .reset_index(drop=True)
        .reset_index(names='Affiliation_Id')
    )

    # Bridge table linking athletes to affiliations
    bridge_athlete_affiliation = (
        df.merge(
            dim_affiliation,
            on=['Affiliation_Club', 'Affiliation_City', 'Affiliation_Country'],
            how='left'
        )[["Athlete_Id", "Affiliation_Id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )


    # dim_affiliation['Affiliations_City'] might contain something like "(POR)"

    # Pattern to match a string like "(XXX)" where X is any letter
    pattern = r'^\(([A-Za-z]{3})\)$'

    mask = dim_affiliation['Affiliation_City'].str.match(pattern, na=False)

    dim_affiliation.loc[mask, 'Affiliation_Country'] = (
        dim_affiliation.loc[mask, 'Affiliation_City']
        .str.extract(pattern)[0]
    )

    dim_affiliation.loc[mask, 'Affiliation_City'] = np.nan
    
    return dim_affiliation, bridge_athlete_affiliation


def roles_parsing(df):

    df = df.copy()

    df['Roles'] = df['Roles'].astype(str).str.strip().replace('—', '').str.split(' • ')
    return df





def noc_parsing(df,countries_df):
        
    df =df.copy()
  
    # Normalize valid country names
    valid_countries = countries_df["English short name lower case"].str.lower().str.strip().tolist()

    # Legacy / historical mappings
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
            return []

        s = noc_str.lower().strip()

        # replace legacy names first
        for old, new in legacy_map.items():
            if old in s:
                s = s.replace(old, new)

        # normalize punctuation and separators
        s = re.sub(r"[/,;]", " ", s)
        s = re.sub(r"\band\b", " ", s)
        s = re.sub(r"\s+", " ", s).strip()

        found = set()

        # try matching full country names first (longest names first to avoid partials)
        for country in sorted(valid_countries, key=len, reverse=True):
            # use boundary-safe regex but require whole phrase match
            pattern = rf'(?<!\w){re.escape(country)}(?!\w)'
            if re.search(pattern, s):
                found.add(country)
                # optionally, remove the matched country from string to avoid overlap
                s = re.sub(pattern, " ", s)

        # Keep unmatched NOC as-is
        if not found:
            return [noc_str.lower().strip()]

        return sorted(found)

    df["NOC"] = df["NOC"].apply(extract_valid_nocs)

    return df


def clean_biodata(df,countries_df):

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



# Results data cleaning


def game_year_type_parsing(df):

    df = df.copy()

    df[['Game Year', 'Game Type']] = df['Games'].str.extract(r'^(\d{4})\s+(.*)$', expand=True)
    df['Game Year'] = pd.to_numeric(df['Game Year'])

    return df

def position_parsing(df):

    df = df.copy()
    
    df['Position'] = df['Pos'].str.extract(r'(\d+)')
    df['Position'] = pd.to_numeric(df['Position'])

    df['Tied'] = df['Pos'].str.contains('=')

    return df




def clean_results(df):
    df = game_year_type_parsing(df)
    df = position_parsing(df)

    columns_to_drop = ['Nationality', 'Unnamed: 7', 'Games', 'Pos']
    df = drop_invalid_columns(df, columns_to_drop)
    return df



# Editions data cleaning

def normalize_competition(text):
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
    df = df.copy()
    df[["Competition_Start", "Competition_End"]] = (
        df["Competition"]
        .str.split("-", n=1, expand=True)  
        .apply(lambda x: x.str.strip())
    )
    return df



def format_date(df, date_cols):
    df = df.copy()
    for col in date_cols:
        df[col] = pd.to_datetime(
            df[col] + " " + df["Year"].astype(str),
            format="%d %B %Y",
            errors="coerce"
        ).dt.strftime("%Y-%m-%d")
    return df
    
def remove_ancient_games(df):
    df = df.copy()
    df = df[df['Game_Type'] != 'Ancient Olympic Games']
    return df

def rename_comments_column(df):
    df = df.copy()
    df = df.rename(columns={'Unnamed: 7':'Comments'})
    return df

def adding_game_id_column(df):

    df = df.copy()
    df = df.sort_values(
        by=["Year", "Game_Type", "Edition_Name"],
        ascending=[True, True, True]
    ).reset_index(drop=True)

    # Add incremental surrogate key (game_id)
    df["Game_Id"] = range(1, len(df) + 1)
    return df



def clean_editions(df):

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



# # if __name__ == "__main__":
# def data_clean_I():

#     try:
#         os.makedirs("../clean_data", exist_ok=True)

#         bios_df = pd.read_csv("../raw_data/biodata.csv")
#         logger.info("Read biodata")
#         countries_df = pd.read_csv("../data/wikipedia-iso-country-codes.csv")
#         logger.info("Read wikipedia-iso-country-codes")

#         results_df = pd.read_csv("../raw_data/results.csv")
#         logger.info("Read results")

#         editions_df = pd.read_csv("../raw_data/editions.csv")
#         logger.info("Read editions")



#         logger.info("Cleaning bios data")
#         bios_df, dim_affiliation_df, bridge_athlete_affiliation_df = clean_biodata(bios_df,countries_df)
#         logger.info(f"Biodata cleaned: {len(bios_df)} rows")
#         logger.info(f"dim_affiliation cleaned: {len(dim_affiliation_df)} rows")
#         logger.info(f"bridge_athlete_affiliation_df cleaned: {len(bridge_athlete_affiliation_df)} rows")

#         logger.info("Cleaning results data")
#         results_df = clean_results(results_df)
#         logger.info(f"Results cleaned: {len(results_df)} rows")

#         logger.info("Cleaning editions data")        
#         editions_df = clean_editions(editions_df)
#         logger.info(f"Editions cleaned: {len(editions_df)} rows")



#         # bios_df.to_parquet('./clean_data/cleaned_biodata.parquet', index=False)
#         # logger.info("Saved bios data")

#         # results_df.to_parquet('./clean_data/cleaned_results.parquet', index=False)
#         # logger.info("Saved results data")

#         # editions_df.to_parquet('./clean_data/cleaned_editions.parquet', index=False)
#         # logger.info("Saved editions data")


#         bios_df.to_csv('../clean_data/cleaned_biodata.csv', index=False)
#         dim_affiliation_df.to_csv('../clean_data/dim_affiliation.csv', index=False)
#         bridge_athlete_affiliation_df.to_csv('../clean_data/bridge_athlete_affiliation.csv', index=False)
#         logger.info("Saved bios data")

#         results_df.to_csv('../clean_data/cleaned_results.csv', index=False)
#         logger.info("Saved results data")

#         editions_df.to_csv('../clean_data/cleaned_editions.csv', index=False)
#         logger.info("Saved editions data")

#     except Exception as e:
#         logger.exception(f"Error during cleaning pipeline: {e}")

        

def data_clean_I():
    """
    Reads raw CSVs from MinIO bronze bucket,
    cleans them, and writes Parquet files to silver bucket.
    """
    # Configuration
    s3_endpoint = "http://minio:9000"  # use "http://host.docker.internal:9000" if Airflow is outside Docker
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

    # Read data from bronze
    logger.info("Reading raw data from bronze bucket...")
    bios_df = pd.read_parquet(f"s3://{bronze_bucket}/raw_data/biodata.parquet", storage_options=s3fs_opts)
    results_df = pd.read_parquet(f"s3://{bronze_bucket}/raw_data/results.parquet", storage_options=s3fs_opts)
    editions_df = pd.read_parquet(f"s3://{bronze_bucket}/raw_data/editions.parquet", storage_options=s3fs_opts)

    countries_df = pd.read_csv(f"s3://{bronze_bucket}/data/wikipedia-iso-country-codes.csv", storage_options=s3fs_opts)

    logger.info("Cleaning biodata...")
    bios_df, dim_affiliation_df, bridge_athlete_affiliation_df = clean_biodata(bios_df, countries_df)
    results_df = clean_results(results_df)
    editions_df = clean_editions(editions_df)

    logger.info("Saving cleaned data to silver bucket as Parquet...")
    bios_df.to_parquet(f"s3://{silver_bucket}/clean_data/cleaned_biodata.parquet", index=False, storage_options=s3fs_opts)
    dim_affiliation_df.to_parquet(f"s3://{silver_bucket}/clean_data/dim_affiliation.parquet", index=False, storage_options=s3fs_opts)
    bridge_athlete_affiliation_df.to_parquet(f"s3://{silver_bucket}/clean_data/bridge_athlete_affiliation.parquet", index=False, storage_options=s3fs_opts)
    results_df.to_parquet(f"s3://{silver_bucket}/clean_data/cleaned_results.parquet", index=False, storage_options=s3fs_opts)
    editions_df.to_parquet(f"s3://{silver_bucket}/clean_data/cleaned_editions.parquet", index=False, storage_options=s3fs_opts)

    logger.info("Data cleaning I complete")
