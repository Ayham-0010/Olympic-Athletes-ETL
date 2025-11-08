import pandas as pd
import pandera.pandas as pa
import numpy as np

from .logger import logger


def df_nan_percentage(df):
    return df.isna().mean().mul(100).round(2).sort_values(ascending=False).map(lambda x: f"{x}%")

def is_list_of_strings(x):
    return all(isinstance(r, str) for r in x)

no_duplicate_name_birth = pa.Check(
    lambda df: ~df.duplicated(subset=["Name", "Born_Date"]),
    element_wise=False,
    error="Duplicate athlete records found with same Name and Born_Date."
)

date_logic = pa.Check(
    lambda df: (df["Born_Date"].isna() | df["Died_Date"].isna()) | (df["Died_Date"] >= df["Born_Date"]),
    element_wise=False,
    error="Died_Date earlier than Born_Date."
)


height_weight_logic = pa.Check(
    lambda df: (
        (df["Weight (kg)"] / ((df["Height (cm)"] / 100) ** 2)).between(15, 45)
    ),
    error="Unrealistic height-to-weight ratio."
)

# --- Schema Definition ---

bios_schema = pa.DataFrameSchema(
    { 
        "Athlete_Id": pa.Column(pd.Int64Dtype, pa.Check.ge(1), nullable=False, unique=True), 
        "Name": pa.Column(str, nullable=False), 
        "Sex": pa.Column(str, pa.Check.isin(["Male", "Female"]), nullable=False), 
        "NOC": pa.Column(str, pa.Check(is_list_of_strings, element_wise=False ), nullable=True),

        "Height (cm)": pa.Column(float, pa.Check.between(100,250), nullable=True), 
        "Weight (kg)": pa.Column(float, pa.Check.between(25,200), nullable=True), 
        "Height_Imputed": pa.Column(bool, nullable=False),
        "Weight_Imputed": pa.Column(bool, nullable=False),

        "Born_Date": pa.Column("datetime64[ns]", nullable=True), 
        "Died_Date": pa.Column("datetime64[ns]", nullable=True),
        'Is_Alive': pa.Column(bool, nullable=False),

        "Born_City": pa.Column(str, nullable=True), 
        "Born_Region": pa.Column(str, nullable=True), 
        "Born_Country": pa.Column(str, pa.Check.str_length(3, 3), nullable=True), 
        "Born_Country_From_NOC": pa.Column(bool, nullable=False),

        "Roles": pa.Column(object,pa.Check(is_list_of_strings, element_wise=False ), nullable=True) 
        
        }, 
        strict=True, 
        coerce=True,
        
        checks=[

            no_duplicate_name_birth,
            date_logic,
            height_weight_logic

            ]

        ) 



duplicate_affiliation_content_check = pa.Check(
    lambda df: ~df.duplicated(subset=["Affiliation_Club"]),
    element_wise=False,
    error="Duplicate affiliations found with different Affiliation_Ids (same club, city, and country)."
)

affiliations_schema = pa.DataFrameSchema(
    { 
        "Affiliation_Id": pa.Column(pd.Int64Dtype, pa.Check.ge(0), nullable=False, unique=True), 

        "Affiliation_Club": pa.Column(str, nullable=True), 
        "Affiliation_City": pa.Column(str, nullable=True), 
        "Affiliation_Country": pa.Column(str, pa.Check.str_length(3, 3), nullable=True), 

        }, 

        strict=True, 
        coerce=True,
        
        checks=[
            duplicate_affiliation_content_check
            ]
        ) 



medal_position_logic_check = pa.Check(
    lambda df: (
        df["Medal"].isna() | df["Position"].isna() | df["Position"] < 3
    ),
    error="Medal assigned to invalid position (must be ≤ 3)."
)

position_medal_match_check = pa.Check(
    lambda df: (
        df["Position"].isna()
        | (
            ((df["Position"] == 1) & (df["Medal"] == "Gold"))
            | ((df["Position"] == 2) & (df["Medal"] == "Silver"))
            | ((df["Position"] == 3) & (df["Medal"] == "Bronze"))
            | (df["Position"] > 3) & (df["Medal"].isna())
        )
    ),
    error="Position–Medal mismatch: check if medal corresponds to rank."
)

# --- Schema Definition ---

results_schema = pa.DataFrameSchema(
    {

        "Athlete_Id": pa.Column(pd.Int64Dtype, pa.Check.ge(1), nullable=False),
        "As":   pa.Column(str, nullable=False),
        "NOC": pa.Column(str, pa.Check.str_length(3, 3), nullable=False),
        "Discipline": pa.Column(str, nullable=False),
        "Game Type": pa.Column(str, nullable=True),
        "Game Year": pa.Column(pd.Int64Dtype, pa.Check.between(1850, 2024), nullable=True),

        "Event": pa.Column(str, nullable=True),
        "Team": pa.Column(str, nullable=True),
        "Tied": pa.Column(bool, nullable=True),

        "Position": pa.Column(pd.Int64Dtype,pa.Check.ge(1), nullable=True),
        "Medal": pa.Column(
            str,
            pa.Check.isin(["Gold", "Silver", "Bronze"]),
            nullable=True
        ),

    },
    strict=True,
    coerce=True,
    checks=[
        medal_position_logic_check,
        position_medal_match_check

    ],

)



game_types_list= ['Olympic Games', 'Intercalated Games', 'Youth Olympic Games','Forerunners to the Olympic Games']
edition_names_list=['Summer', 'Winter', 'Equestrian']

no_duplicate_games_check = pa.Check(
    lambda df: ~df.duplicated(subset=["Year", "Edition_Name", "Game_Type"]),
    element_wise=False,
    error="Duplicate game editions detected based on Year, Edition_Name, and Game_Type."
)

edition_date_check = pa.Check(
    lambda df: (
        # Opened <= Closed  (or either missing)
        df["Opened"].isna() | df["Closed"].isna() | (df["Opened"] <= df["Closed"])
    ),
    
    error="Chronological order violated: check Opened, Closed edition dates."
)

Competition_date_check = pa.Check(
    lambda df: (

        # Competition_Start <= Competition_End  (or either missing)
        df["Competition_Start"].isna() | df["Competition_End"].isna() | (df["Competition_Start"] <= df["Competition_End"])
        
    ),

    error="Chronological order violated:check Start, End Competition dates."
)


# --- Schema Definition ---
editions_schema = pa.DataFrameSchema(
    {   
        "game_id": pa.Column(pd.Int64Dtype, pa.Check.ge(1), nullable=False),
        "Year": pa.Column(pd.Int64Dtype, pa.Check.between(1850, 2024), nullable=False),
        "Game_Type": pa.Column(str,pa.Check.isin(game_types_list), nullable=False),
        "Edition_Name": pa.Column(str,pa.Check.isin(edition_names_list), nullable=True),

        "City": pa.Column(str, nullable=False),
        "Country": pa.Column(str, nullable=False),

        "Opened": pa.Column("datetime64[ns]", nullable=True),
        "Closed": pa.Column("datetime64[ns]", nullable=True),

        "Competition_Start": pa.Column("datetime64[ns]", nullable=True),
        "Competition_End": pa.Column("datetime64[ns]", nullable=True),

        "Comments": pa.Column(str, nullable=True),
        
        "Opened_Imputed": pa.Column(bool, nullable=False),	
        "Closed_Imputed": pa.Column(bool, nullable=False),
        "Competition_Start_Imputed": pa.Column(bool, nullable=False),
        "Competition_End_Imputed": pa.Column(bool, nullable=False),

    },
    strict=True,
    coerce=True,
    checks=[
        no_duplicate_games_check,
        edition_date_check,
        Competition_date_check,
        # edition_Competition_date_check      # ensure chronological order

    ],
    name="games_schema"
)


# def get_error_df(df, original_df):
#     # Example df
#     # df has columns ['failure_case', 'column', 'check']

#     # Step 1: Find max duplication count
#     max_count = df['column'].value_counts().max()

#     # Step 2: Keep only columns with max_count occurrences
#     valid_columns = df['column'].value_counts()[df['column'].value_counts() == max_count].index
#     filtered = df[df['column'].isin(valid_columns)]

#     # Step 3: Get all unique checks
#     all_checks = filtered['check'].unique()

#     # Step 4: Transform each check separately and store results
#     dfs = []

#     for chk in all_checks:
#         chk_filtered = filtered[filtered['check'] == chk]
        
#         # Group by column, collect failure_case
#         grouped = chk_filtered.groupby('column')['failure_case'].apply(list)
        
#         # Create wide DataFrame
#         temp_df = pd.DataFrame({col: vals for col, vals in grouped.items()})
        
#         # Add failed_check column
#         temp_df['failed_check'] = chk
        
#         # Append to list
#         dfs.append(temp_df)

#     # Step 5: Concatenate all check-specific DataFrames
#     wide_df = pd.concat(dfs, ignore_index=True)

#     # Step 1: Identify missing columns
#     # Exclude 'failed_check' if it already exists in wide_df
#     missing_cols = [c for c in original_df.columns if c not in wide_df.columns and c != 'failed_check']

#     # Step 2: Merge missing columns back
#     # We'll use the columns that exist in wide_df (except 'failed_check') as keys
#     merge_cols = [c for c in wide_df.columns if c != 'failed_check']

#     # Step 3: Merge wide_df with original_df to get missing columns
#     # Using left join to keep all rows in wide_df
#     final_df = pd.merge(
#         wide_df,
#         original_df[merge_cols + missing_cols].drop_duplicates(),
#         on=merge_cols,
#         how='left'
#     )

#     # Step 4: Optional: check the result

#     return final_df.drop_duplicates()

def get_error_df(df, original_df):
    """
    Transform an error DataFrame to a wide format and merge missing columns
    from the original DataFrame safely.

    Args:
        df (pd.DataFrame): Error DataFrame with columns ['failure_case', 'column', 'check']
        original_df (pd.DataFrame): Original full DataFrame

    Returns:
        pd.DataFrame: Clean wide-format error DataFrame with all original columns
    """

    # Step 1: Find max duplication count for columns
    max_count = df['column'].value_counts().max()

    # Step 2: Keep only columns with max_count occurrences
    valid_columns = df['column'].value_counts()[df['column'].value_counts() == max_count].index
    filtered = df[df['column'].isin(valid_columns)]

    # Step 3: Get all unique checks
    all_checks = filtered['check'].unique()

    # Step 4: Transform each check separately
    dfs = []

    for chk in all_checks:
        chk_filtered = filtered[filtered['check'] == chk]

        # Group by column, collect failure_case as list
        grouped = chk_filtered.groupby('column')['failure_case'].apply(list)

        # Convert lists to strings to avoid unhashable issues
        temp_df = pd.DataFrame({col: ['; '.join(map(str, vals))] for col, vals in grouped.items()})

        # Add failed_check column
        temp_df['failed_check'] = chk

        dfs.append(temp_df)

    # Step 5: Concatenate all check-specific DataFrames
    wide_df = pd.concat(dfs, ignore_index=True)

    # Step 6: Identify missing columns from original_df
    missing_cols = [c for c in original_df.columns if c not in wide_df.columns and c != 'failed_check']

    # Step 7: Prepare original_df subset for merging
    original_df_fixed = original_df[wide_df.columns.drop('failed_check').tolist() + missing_cols].copy()

    # Step 7a: Convert any list/array cells to strings
    for col in original_df_fixed.columns:
        original_df_fixed[col] = original_df_fixed[col].apply(
            lambda x: '; '.join(map(str, x)) if isinstance(x, (list, np.ndarray)) else x
        )

    # Step 7b: Ensure merge columns have same type as wide_df
    merge_cols = [c for c in wide_df.columns if c != 'failed_check']
    for col in merge_cols:
        original_df_fixed[col] = original_df_fixed[col].astype(str)

    # Step 8: Merge wide_df with original_df_fixed
    final_df = pd.merge(
        wide_df,
        original_df_fixed.drop_duplicates(),
        on=merge_cols,
        how='left'
    )

    # Step 9: Remove duplicates and return
    return final_df.drop_duplicates()



# # if __name__ == "__main__":
# def data_validation_quality_checks():


#     logger.info("Loading data...")

#     # Load your data
#     bios_df = pd.read_csv("./clean_data_II/cleaned_biodata.csv")
#     results_df = pd.read_csv('./clean_data/cleaned_results.csv')
#     editions_df = pd.read_csv('./clean_data_II/cleaned_editions.csv')
#     affiliation_df = pd.read_csv("./clean_data/dim_affiliation.csv")

#     # Validate bios_df
#     try: 
#         bios_schema.validate(bios_df, lazy=True) 
#         logger.info("Bios validation PASSED!")
#     except pa.errors.SchemaErrors as exc: 
#         logger.error("Bios validation FAILED!")
#         bios_error_df = exc.failure_cases

#     # Validate affiliation_df
#     try: 
#         affiliations_schema.validate(affiliation_df, lazy=True) 
#         logger.info("Affiliations validation PASSED!") 
#     except pa.errors.SchemaErrors as exc: 
#         logger.error("Affiliations validation FAILED!")
#         affiliations_error_df = exc.failure_cases

#     # Validate editions_df
#     try:
#         editions_schema.validate(editions_df, lazy=True)
#         logger.info("Editions validation PASSED!")
#     except pa.errors.SchemaErrors as exc:
#         logger.error("Editions validation FAILED!")
#         editions_error_df = exc.failure_cases

#     # Validate results_df
#     try:
#         results_schema.validate(results_df, lazy=True)
#         logger.info("Results validation PASSED!")
#     except pa.errors.SchemaErrors as exc:
#         logger.error("Results validation FAILED!")
#         results_error_df = exc.failure_cases

#     # Process and log error dataframes
#     bios_error_df = bios_error_df[["failure_case", "column", 'check']].sort_index()
#     bios_failure_cases_df = get_error_df(bios_error_df, bios_df)
#     logger.info("Bios failure cases dataframe created.")

#     affiliations_error_df = affiliations_error_df[["failure_case", "column", 'check']].sort_index()
#     affiliations_failure_cases_df = get_error_df(affiliations_error_df, affiliation_df)
#     logger.info("Affiliations failure cases dataframe created.")

#     editions_error_df = editions_error_df[["failure_case", "column", 'check']].sort_index()
#     editions_failure_cases_df = get_error_df(editions_error_df, editions_df)
#     logger.info("Editions failure cases dataframe created.")

#     results_error_df = results_error_df[["failure_case", "column", 'check']].sort_index()
#     results_failure_cases_df = get_error_df(results_error_df, results_df)
#     logger.info("Results failure cases dataframe created.")


#     bios_failure_cases_df.to_csv('./failure_cases/bios_failure_cases.csv', index=False)
#     affiliations_failure_cases_df.to_csv('./failure_cases/affiliations_failure_cases.csv', index=False)
#     editions_failure_cases_df.to_csv('./failure_cases/editions_failure_cases.csv', index=False)
#     results_failure_cases_df.to_csv('./failure_cases/results_failure_cases.csv', index=False)
#     logger.info("failure cases data saved.")


def data_validation_quality_checks():
    """
    Loads cleaned data from silver bucket,
    performs validation (omitted here),
    and saves failure cases back to silver bucket.
    """
    # Configuration
    s3_endpoint = "http://minio:9000"  # or host/IP if outside Docker
    access_key = "accesskey"
    secret_key = "secretkey"
    silver_bucket = "silver"

    s3fs_opts = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": s3_endpoint},
    }

    logger.info("Loading data from silver bucket...")

    bios_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data_II/cleaned_biodata.parquet", storage_options=s3fs_opts)
    results_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data/cleaned_results.parquet", storage_options=s3fs_opts)
    editions_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data_II/cleaned_editions.parquet", storage_options=s3fs_opts)
    affiliation_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data/dim_affiliation.parquet", storage_options=s3fs_opts)

   # Validate bios_df
    try: 
        bios_schema.validate(bios_df, lazy=True) 
        logger.info("Bios validation PASSED!")
    except pa.errors.SchemaErrors as exc: 
        logger.error("Bios validation FAILED!")
        bios_error_df = exc.failure_cases

    # Validate affiliation_df
    try: 
        affiliations_schema.validate(affiliation_df, lazy=True) 
        logger.info("Affiliations validation PASSED!") 
    except pa.errors.SchemaErrors as exc: 
        logger.error("Affiliations validation FAILED!")
        affiliations_error_df = exc.failure_cases

    # Validate editions_df
    try:
        editions_schema.validate(editions_df, lazy=True)
        logger.info("Editions validation PASSED!")
    except pa.errors.SchemaErrors as exc:
        logger.error("Editions validation FAILED!")
        editions_error_df = exc.failure_cases

    # Validate results_df
    try:
        results_schema.validate(results_df, lazy=True)
        logger.info("Results validation PASSED!")
    except pa.errors.SchemaErrors as exc:
        logger.error("Results validation FAILED!")
        results_error_df = exc.failure_cases

    # Process and log error dataframes
    bios_error_df = bios_error_df[["failure_case", "column", 'check']].sort_index()
    bios_failure_cases_df = get_error_df(bios_error_df, bios_df)
    logger.info("Bios failure cases dataframe created.")

    affiliations_error_df = affiliations_error_df[["failure_case", "column", 'check']].sort_index()
    affiliations_failure_cases_df = get_error_df(affiliations_error_df, affiliation_df)
    logger.info("Affiliations failure cases dataframe created.")

    editions_error_df = editions_error_df[["failure_case", "column", 'check']].sort_index()
    editions_failure_cases_df = get_error_df(editions_error_df, editions_df)
    logger.info("Editions failure cases dataframe created.")

    results_error_df = results_error_df[["failure_case", "column", 'check']].sort_index()
    results_failure_cases_df = get_error_df(results_error_df, results_df)
    logger.info("Results failure cases dataframe created.")


    logger.info("Saving failure cases to silver bucket...")

    bios_failure_cases_df.to_parquet(f"s3://{silver_bucket}/failure_cases/bios_failure_cases.parquet", index=False, storage_options=s3fs_opts)
    affiliations_failure_cases_df.to_parquet(f"s3://{silver_bucket}/failure_cases/affiliations_failure_cases.parquet", index=False, storage_options=s3fs_opts)
    editions_failure_cases_df.to_parquet(f"s3://{silver_bucket}/failure_cases/editions_failure_cases.parquet", index=False, storage_options=s3fs_opts)
    results_failure_cases_df.to_parquet(f"s3://{silver_bucket}/failure_cases/results_failure_cases.parquet", index=False, storage_options=s3fs_opts)

    logger.info("Failure cases saved to silver bucket")