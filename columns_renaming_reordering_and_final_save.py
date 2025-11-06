import pandas as pd
from logger import logger

def rename_and_reorder_df_columns(df: pd.DataFrame, names_dict: dict) -> pd.DataFrame:

    df = df.copy()
    df = df.rename(columns=names_dict)
    
    # Reorder columns according to dict values (ignore missing)
    ordered_cols = [col for col in names_dict.values() if col in df.columns]
    df = df[ordered_cols]
    
    return df




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

Events_fct_table_columns_names = {
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


if __name__ == "__main__":

    logger.info("Loading data...")

    # Load your data
    bios_df = pd.read_csv("./clean_data_II/cleaned_biodata.csv")
    results_df = pd.read_csv('./clean_data/cleaned_results.csv')
    editions_df = pd.read_csv('./clean_data_II/cleaned_editions.csv')
    affiliation_df = pd.read_csv("./clean_data/dim_affiliation.csv")
    bridge_athlete_affiliation_df = pd.read_csv("./clean_data/bridge_athlete_affiliation.csv")

    dim_athletes_df = rename_and_reorder_df_columns(bios_df, athlete_dim_table_columns_names)
    dim_affiliations_df = rename_and_reorder_df_columns(affiliation_df, affiliation_dim_table_columns_names)
    bridge_athletes_affiliations_df = rename_and_reorder_df_columns(bridge_athlete_affiliation_df, athlete_affiliation_bridge_table_columns_names)
    dim_games_df = rename_and_reorder_df_columns(editions_df, games_dim_table_columns_names)
    fct_results_df = rename_and_reorder_df_columns(results_df, Events_fct_table_columns_names)

    logger.info("Renamed and ordered all dataframes according to DW conventions")


    dim_athletes_df.to_csv('./data_clean_final/dim_athletes.csv', index=False)
    dim_affiliations_df.to_csv('./data_clean_final/dim_affiliations.csv', index=False)
    bridge_athletes_affiliations_df.to_csv('./data_clean_final/bridge_athletes_affiliations.csv', index=False)
    dim_games_df.to_csv('./data_clean_final/dim_games.csv', index=False)
    fct_results_df.to_csv('./data_clean_final/fct_results.csv', index=False)
    logger.info("final cleaned data saved.")

