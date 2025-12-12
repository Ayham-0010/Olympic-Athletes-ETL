import pandas as pd
import numpy as np

from .logger import logger

def impute_height_weight_by_discipline(bio_df, results_df):
    """
    Imputes missing athlete height and weight using discipline-specific medians.

    Strategy:
      1. Assign each athlete their most frequent discipline.
      2. Impute missing Height/Weight using median by (Sex, Discipline).
      3. Fallback to median by Sex if discipline median unavailable.
      4. Add flags indicating where imputations occurred.
    """

    bio = bio_df.copy()

    # Determine most frequent discipline per athlete
    discipline_map = (
        results_df.groupby('Athlete_Id')['Discipline']
        .apply(lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan)
        .reset_index()
    )
    bio = bio.merge(discipline_map, on='Athlete_Id', how='left')


    # Create imputation flags before filling

    bio['Height_Imputed'] = bio['Height (cm)'].isna()
    bio['Weight_Imputed'] = bio['Weight (kg)'].isna()


    # Fill missing using median by (Sex, Discipline)
    grp = bio.groupby(['Sex', 'Discipline'])
    height_med_sex_disc = grp['Height (cm)'].transform('median')
    weight_med_sex_disc = grp['Weight (kg)'].transform('median')

    bio['Height (cm)'] = bio['Height (cm)'].fillna(height_med_sex_disc)
    bio['Weight (kg)'] = bio['Weight (kg)'].fillna(weight_med_sex_disc)


    # Fallback: median by Sex only
    bio['Height (cm)'] = bio.groupby('Sex')['Height (cm)'].transform(lambda x: x.fillna(x.median()))
    bio['Weight (kg)'] = bio.groupby('Sex')['Weight (kg)'].transform(lambda x: x.fillna(x.median()))


    # Update flags to capture fallback imputations
    orig_height_na = bio_df['Height (cm)'].isna()
    orig_weight_na = bio_df['Weight (kg)'].isna()

    bio['Height_Imputed'] = bio['Height_Imputed'] | (orig_height_na & bio['Height (cm)'].notna())
    bio['Weight_Imputed'] = bio['Weight_Imputed'] | (orig_weight_na & bio['Weight (kg)'].notna())


    # Cleanup intermediate column
    bio = bio.drop(columns=['Discipline'])

    return bio



def impute_Born_Country_by_NOC(bios_df, iso_df):
    """
    Imputes missing Born_Country values using athlete NOC codes.
    Uses ISO country codes for standardized country mapping.
    """
    
    bios_df = bios_df.copy()
    iso_df = iso_df.copy()

    # Map country name - Alpha-3 code
    country_to_code = dict(zip(
        iso_df['English short name lower case'].str.lower(),
        iso_df['Alpha-3 code']
    ))

    # Helper: derive first country’s Alpha-3 code from NOC string
    def get_noc_code(noc_str):
        if pd.isna(noc_str) or noc_str.strip() == '':
            return np.nan

        first_country = noc_str.split(',')[0].strip()
        return country_to_code.get(first_country.lower(), np.nan)

    # # Flag where Born_Country is imputed
    bios_df['Born_Country_From_NOC'] = bios_df['Born_Country'].isna()

    # Impute missing Born_Country based on NOC
    bios_df['Born_Country'] = bios_df.apply(
        lambda row: get_noc_code(row['NOC']) if pd.isna(row['Born_Country']) else row['Born_Country'],
        axis=1
    )

    return bios_df


def impute_games_and_competition_dates(df):
    """
    Fills missing Games and Competition date fields where possible:
      - If 'Opened' missing but 'Competition_Start' present, fill 'Opened' 
      - If 'Closed' missing but 'Competition_End' present, fill 'Closed'
    """

    df = df.copy()

    # Initialize all imputation flags to False
    df["Opened_Imputed"] = False
    df["Closed_Imputed"] = False

    # Identify missing - available pairs
    opened_from_start_mask = df["Opened"].isna() & df["Competition_Start"].notna()
    closed_from_end_mask = df["Closed"].isna() & df["Competition_End"].notna()

    # # Perform imputations
    df.loc[opened_from_start_mask, "Opened"] = df.loc[opened_from_start_mask, "Competition_Start"]
    df.loc[closed_from_end_mask, "Closed"] = df.loc[closed_from_end_mask, "Competition_End"]

    # Mark imputed records
    df.loc[opened_from_start_mask, "Opened_Imputed"] = True
    df.loc[closed_from_end_mask, "Closed_Imputed"] = True

    return df


def data_clean_II():
    """
    Stage II of the data cleaning pipeline.

    Reads cleaned datasets from the Silver (clean_data) layer,
    performs targeted imputations for missing attributes, 
    and writes updated Parquet files to Silver (clean_data_II).
    """

    # MinIO configuration
    s3_endpoint = "http://minio:9000"
    access_key = "accesskey"
    secret_key = "secretkey"
    silver_bucket = "silver"
    bronze_bucket= "bronze"

    # S3-compatible options for s3fs
    s3fs_opts = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": s3_endpoint},
    }

    logger.info("Loading cleaned data from silver bucket...")

    # Load cleaned datasets
    bios_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data/cleaned_biodata.parquet", storage_options=s3fs_opts)
    results_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data/cleaned_results.parquet", storage_options=s3fs_opts)
    editions_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data/cleaned_editions.parquet", storage_options=s3fs_opts)
    iso_df = pd.read_csv(f"s3://{bronze_bucket}/data/wikipedia-iso-country-codes.csv", storage_options=s3fs_opts)

    # Apply imputations
    bios_df = impute_height_weight_by_discipline(bios_df, results_df)
    bios_df = impute_Born_Country_by_NOC(bios_df, iso_df)
    editions_df = impute_games_and_competition_dates(editions_df)

    logger.info("Imputation complete")

    # Save imputed datasets
    bios_df.to_parquet(f"s3://{silver_bucket}/clean_data_II/cleaned_biodata.parquet", index=False, storage_options=s3fs_opts)
    editions_df.to_parquet(f"s3://{silver_bucket}/clean_data_II/cleaned_editions.parquet", index=False, storage_options=s3fs_opts)

    logger.info("Data Cleaning II process completed successfully.")