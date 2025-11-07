import pandas as pd
import numpy as np
import ast

from logger import logger

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


def impute_Born_Country_by_NOC(bios_df):
    # clean_bio_df = pd.read_csv('your_bio_df.csv')  # assuming it's already loaded
    bios_df = bios_df.copy()
    # Load the ISO country codes CSV
    iso_df = pd.read_csv('./data/wikipedia-iso-country-codes.csv')

    # Create a mapping from lowercase English short name to Alpha-3 code
    country_to_code = dict(zip(iso_df['English short name lower case'].str.lower(), iso_df['Alpha-3 code']))

    # Function to get the first NOC country code
    def get_noc_code(noc_list):
        noc_list = ast.literal_eval(noc_list)
        if len(noc_list) > 0:
            return country_to_code.get(noc_list[0])
        return np.nan

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

if __name__ == "__main__":

    logger.info("Loading data...")
    # Load your data
    bios_df = pd.read_csv("./clean_data/cleaned_biodata.csv")
    results_df = pd.read_csv('./clean_data/cleaned_results.csv')
    editions_df = pd.read_csv('./clean_data/cleaned_editions.csv')
    affiliation_df = pd.read_csv("./clean_data/dim_affiliation.csv")


    # Impute!
    bios_df = impute_height_weight_by_discipline(bios_df, results_df)
    
    bios_df = impute_Born_Country_by_NOC(bios_df)
    logger.info("imputed missing  bios height, weight, and Born Country data")
    editions_df = impute_games_and_competition_dates(editions_df)
    logger.info("imputed missing  editions games and competition dates data")


    bios_df.to_csv('./clean_data_II/cleaned_biodata.csv', index=False)
    editions_df.to_csv('./clean_data_II/cleaned_editions.csv', index=False)
    logger.info("saved the imputed data")