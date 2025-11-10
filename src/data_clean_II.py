import pandas as pd
import numpy as np
import ast

from .logger import logger

def impute_height_weight_by_discipline(bio_df, results_df):

    bio = bio_df.copy()

    # -------------------------------------------------
    # 1. ONE DISCIPLINE PER ATHLETE (most frequent)
    # -------------------------------------------------
    discipline_map = (
        results_df.groupby('Athlete_Id')['Discipline']
        .apply(lambda s: s.mode().iloc[0] if not s.mode().empty else np.nan)
        .reset_index()
    )
    bio = bio.merge(discipline_map, on='Athlete_Id', how='left')

    # -------------------------------------------------
    # 2. IMPUTATION FLAGS (before any filling)
    # -------------------------------------------------
    bio['Height_Imputed'] = bio['Height (cm)'].isna()
    bio['Weight_Imputed'] = bio['Weight (kg)'].isna()

    # -------------------------------------------------
    # 3. MEDIANS BY Sex + Discipline
    # -------------------------------------------------
    grp = bio.groupby(['Sex', 'Discipline'])
    height_med_sex_disc = grp['Height (cm)'].transform('median')
    weight_med_sex_disc = grp['Weight (kg)'].transform('median')

    # Fill with Sex+Discipline median where possible
    bio['Height (cm)'] = bio['Height (cm)'].fillna(height_med_sex_disc)
    bio['Weight (kg)'] = bio['Weight (kg)'].fillna(weight_med_sex_disc)

    # -------------------------------------------------
    # 4. FALLBACK: MEDIAN BY Sex ONLY
    # -------------------------------------------------
    bio['Height (cm)'] = bio.groupby('Sex')['Height (cm)'].transform(
        lambda x: x.fillna(x.median())
    )
    bio['Weight (kg)'] = bio.groupby('Sex')['Weight (kg)'].transform(
        lambda x: x.fillna(x.median())
    )

    # -------------------------------------------------
    # 5. UPDATE FLAGS FOR FALLBACK IMPUTATIONS
    # -------------------------------------------------
    # Original missing values that are now filled
    orig_height_na = bio_df['Height (cm)'].isna()
    orig_weight_na = bio_df['Weight (kg)'].isna()

    bio['Height_Imputed'] = bio['Height_Imputed'] | (orig_height_na & bio['Height (cm)'].notna())
    bio['Weight_Imputed'] = bio['Weight_Imputed'] | (orig_weight_na & bio['Weight (kg)'].notna())

    # -------------------------------------------------
    # 6. CLEAN-UP
    # -------------------------------------------------
    bio = bio.drop(columns=['Discipline'])

    return bio



def impute_Born_Country_by_NOC(bios_df, iso_df):
    """
    Imputes the 'Born_Country' column in bios_df using the first NOC code from
    the 'NOC' column (comma-separated string) and a mapping from iso_df.
    
    Parameters:
        bios_df: DataFrame containing 'NOC' column as comma-separated strings
        iso_df: DataFrame containing ISO country codes with columns:
                'English short name lower case' and 'Alpha-3 code'
                
    Returns:
        bios_df with imputed 'Born_Country' where missing
    """
    import numpy as np
    
    bios_df = bios_df.copy()
    iso_df = iso_df.copy()

    # Create a mapping from lowercase English short name to Alpha-3 code
    country_to_code = dict(zip(
        iso_df['English short name lower case'].str.lower(),
        iso_df['Alpha-3 code']
    ))

    # Function to get the first NOC country code
    def get_noc_code(noc_str):
        if pd.isna(noc_str) or noc_str.strip() == '':
            return np.nan
        # Take the first country (comma-separated)
        first_country = noc_str.split(',')[0].strip()
        # Map to Alpha-3 code
        return country_to_code.get(first_country.lower(), np.nan)

    # Impute Born_Country where missing
    bios_df['Born_Country_From_NOC'] = bios_df['Born_Country'].isna()
    bios_df['Born_Country'] = bios_df.apply(
        lambda row: get_noc_code(row['NOC']) if pd.isna(row['Born_Country']) else row['Born_Country'],
        axis=1
    )

    return bios_df
# def impute_Born_Country_by_NOC(bios_df, iso_df):
#     """
#     Impute Born_Country values based on NOC (National Olympic Committee) codes.
#     Handles NOC values that may be strings, lists, arrays, or NaN.
#     """

#     bios_df = bios_df.copy()
#     iso_df = iso_df.copy()

#     # Create mapping from lowercase English short name to Alpha-3 code
#     country_to_code = dict(
#         zip(
#             iso_df["English short name lower case"].str.lower(),
#             iso_df["Alpha-3 code"]
#         )
#     )

#     # --- Robust helper function ---
#     def get_noc_code(noc_list):
#         # Handle None or NaN
#         if noc_list is None or (isinstance(noc_list, float) and np.isnan(noc_list)):
#             return np.nan

#         # If it's already a list or numpy array
#         if isinstance(noc_list, (list, np.ndarray)):
#             items = list(noc_list)
#         elif isinstance(noc_list, str):
#             # Try parsing a string that may look like a list
#             try:
#                 parsed = ast.literal_eval(noc_list)
#                 if isinstance(parsed, (list, np.ndarray)):
#                     items = list(parsed)
#                 else:
#                     items = [parsed]
#             except Exception:
#                 # Fallback: plain string, treat it as a single item
#                 items = [noc_list]
#         else:
#             # Fallback for any unexpected type (e.g., int)
#             items = [noc_list]

#         if len(items) > 0:
#             country = str(items[0]).strip().lower()
#             return country_to_code.get(country, np.nan)

#         return np.nan

    # Create a flag column to indicate when Born_Country is derived from NOC
    bios_df['Born_Country_From_NOC'] = False

    # Apply the function only where Born_Country is missing
    mask = bios_df['Born_Country'].isna()
    bios_df.loc[mask, 'Born_Country'] = bios_df.loc[mask, 'NOC'].apply(get_noc_code)
    bios_df.loc[mask, 'Born_Country_From_NOC'] = True
    
    return bios_df



def impute_games_and_competition_dates(df):
    df = df.copy()

    # Initialize flag columns as False (no imputation by default)
    df["Opened_Imputed"] = False
    df["Closed_Imputed"] = False
    df["Competition_Start_Imputed"] = False
    df["Competition_End_Imputed"] = False

    # --- Create masks before imputation ---
    opened_from_start_mask = df["Opened"].isna() & df["Competition_Start"].notna()
    closed_from_end_mask = df["Closed"].isna() & df["Competition_End"].notna()
    start_from_opened_mask = df["Competition_Start"].isna() & df["Opened"].notna()
    end_from_closed_mask = df["Competition_End"].isna() & df["Closed"].notna()

    # --- Apply imputations ---
    df.loc[start_from_opened_mask, "Competition_Start"] = df.loc[start_from_opened_mask, "Opened"]
    df.loc[end_from_closed_mask, "Competition_End"] = df.loc[end_from_closed_mask, "Closed"]
    df.loc[opened_from_start_mask, "Opened"] = df.loc[opened_from_start_mask, "Competition_Start"]
    df.loc[closed_from_end_mask, "Closed"] = df.loc[closed_from_end_mask, "Competition_End"]

    # --- Set flags where imputations occurred ---
    df.loc[opened_from_start_mask, "Opened_Imputed"] = True
    df.loc[closed_from_end_mask, "Closed_Imputed"] = True
    df.loc[start_from_opened_mask, "Competition_Start_Imputed"] = True
    df.loc[end_from_closed_mask, "Competition_End_Imputed"] = True

    return df

# # if __name__ == "__main__":
# def data_clean_II():

#     logger.info("Loading data...")
#     # Load your data
#     bios_df = pd.read_csv("./clean_data/cleaned_biodata.csv")
#     results_df = pd.read_csv('./clean_data/cleaned_results.csv')
#     editions_df = pd.read_csv('./clean_data/cleaned_editions.csv')



#     # Impute!
#     bios_df = impute_height_weight_by_discipline(bios_df, results_df)
    
#     bios_df = impute_Born_Country_by_NOC(bios_df)
#     logger.info("imputed missing  bios height, weight, and Born Country data")
#     editions_df = impute_games_and_competition_dates(editions_df)
#     logger.info("imputed missing  editions games and competition dates data")

#     # Save
#     bios_df.to_csv('./clean_data_II/cleaned_biodata.csv', index=False)
#     editions_df.to_csv('./clean_data_II/cleaned_editions.csv', index=False)
#     logger.info("saved the imputed data")



def data_clean_II():
    """
    Reads cleaned Parquet data from MinIO silver bucket,
    imputes missing values, and writes back as Parquet.
    """
    # Configuration
    s3_endpoint = "http://minio:9000"  # use your host/IP if needed
    access_key = "accesskey"
    secret_key = "secretkey"
    silver_bucket = "silver"
    bronze_bucket= "bronze"

    # s3fs-compatible storage options
    s3fs_opts = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": s3_endpoint},
    }

    logger.info("Loading cleaned data from silver bucket...")

    # Load Parquet files from silver/clean_data
    bios_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data/cleaned_biodata.parquet", storage_options=s3fs_opts)
    results_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data/cleaned_results.parquet", storage_options=s3fs_opts)
    editions_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data/cleaned_editions.parquet", storage_options=s3fs_opts)
    iso_df = pd.read_csv(f"s3://{bronze_bucket}/data/wikipedia-iso-country-codes.csv", storage_options=s3fs_opts)

    # Impute missing values
    bios_df = impute_height_weight_by_discipline(bios_df, results_df)
    bios_df = impute_Born_Country_by_NOC(bios_df, iso_df)
    editions_df = impute_games_and_competition_dates(editions_df)

    logger.info("Imputation complete")

    # Save imputed data back to silver bucket
    bios_df.to_parquet(f"s3://{silver_bucket}/clean_data_II/cleaned_biodata.parquet", index=False, storage_options=s3fs_opts)
    editions_df.to_parquet(f"s3://{silver_bucket}/clean_data_II/cleaned_editions.parquet", index=False, storage_options=s3fs_opts)

    logger.info("Saved imputed data to silver bucket")