import pandas as pd
import numpy as np
import re, os

from logger import logger



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
    extracted.columns = ['Affiliations_Club', 'Affiliations_City', 'Affiliations_Country']

    df = pd.concat([df[['Athlete_Id']], extracted], axis=1)

    # Drop duplicates to get unique affiliations
    dim_affiliation = (
        df[['Affiliations_Club', 'Affiliations_City', 'Affiliations_Country']]
        .drop_duplicates()
        .reset_index(drop=True)
        .reset_index(names='Affiliation_Id')
    )

    # Bridge table linking athletes to affiliations
    bridge_athlete_affiliation = (
        df.merge(
            dim_affiliation,
            on=['Affiliations_Club', 'Affiliations_City', 'Affiliations_Country'],
            how='left'
        )[["Athlete_Id", "Affiliation_Id"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )


    # dim_affiliation['Affiliations_City'] might contain something like "(POR)"

    # Pattern to match a string like "(XXX)" where X is any letter
    pattern = r'^\(([A-Za-z]{3})\)$'

    mask = dim_affiliation['Affiliations_City'].str.match(pattern, na=False)

    dim_affiliation.loc[mask, 'Affiliations_Country'] = (
        dim_affiliation.loc[mask, 'Affiliations_City']
        .str.extract(pattern)[0]
    )

    dim_affiliation.loc[mask, 'Affiliations_City'] = np.nan
    
    return dim_affiliation, bridge_athlete_affiliation


def roles_parsing(df):

    df = df.copy()

    df['Roles'] = df['Roles'].astype(str).str.strip().replace('—', '').str.split(' • ')
    return df



def clean_biodata(df):

    df = name_parsing(df)
    df = measurements_parsing(df)
    df = date_parsing(df)
    df = location_parsing(df)
    df = roles_parsing(df)

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

    return df




if __name__ == "__main__":

    try:
        os.makedirs("./clean_data", exist_ok=True)

        bios_df = pd.read_csv("./raw_data/biodata.csv")
        logger.info("Read biodata.csv")

        results_df = pd.read_csv("./raw_data/results.csv")
        logger.info("Read results.csv")

        editions_df = pd.read_csv("./raw_data/editions.csv")
        logger.info("Read editions.csv")



        logger.info("Cleaning bios data")
        bios_df, dim_affiliation_df, bridge_athlete_affiliation_df = clean_biodata(bios_df)
        logger.info(f"Biodata cleaned: {len(bios_df)} rows")
        logger.info(f"dim_affiliation cleaned: {len(dim_affiliation_df)} rows")
        logger.info(f"bridge_athlete_affiliation_df cleaned: {len(dim_affiliation_df)} rows")

        logger.info("Cleaning results data")
        results_df = clean_results(results_df)
        logger.info(f"Results cleaned: {len(results_df)} rows")

        logger.info("Cleaning editions data")        
        editions_df = clean_editions(editions_df)
        logger.info(f"Editions cleaned: {len(editions_df)} rows")



        # bios_df.to_parquet('./clean_data/cleaned_biodata.parquet', index=False)
        # logger.info("Saved bios data")

        # results_df.to_parquet('./clean_data/cleaned_results.parquet', index=False)
        # logger.info("Saved results data")

        # editions_df.to_parquet('./clean_data/cleaned_editions.parquet', index=False)
        # logger.info("Saved editions data")


        bios_df.to_csv('./clean_data/cleaned_biodata.csv', index=False)
        dim_affiliation_df.to_csv('./clean_data/dim_affiliation.csv', index=False)
        bridge_athlete_affiliation_df.to_csv('./clean_data/bridge_athlete_affiliation.csv', index=False)
        logger.info("Saved bios data")

        results_df.to_csv('./clean_data/cleaned_results.csv', index=False)
        logger.info("Saved results data")

        editions_df.to_csv('./clean_data/cleaned_editions.csv', index=False)
        logger.info("Saved editions data")

    except Exception as e:
        logger.exception(f"Error during cleaning pipeline: {e}")

        
