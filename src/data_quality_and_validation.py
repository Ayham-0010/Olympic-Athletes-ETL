import pandas as pd
import pandera.pandas as pa

from .logger import logger


# Helper Functions

def df_nan_percentage(df):
    """Return a Series showing percentage of NaN values per column."""

    return df.isna().mean().mul(100).round(2).sort_values(ascending=False).map(lambda x: f"{x}%")

def is_list_of_strings(x):
    """Return True if all elements in iterable are strings."""

    return all(isinstance(r, str) for r in x)



# Bios Pandera Checks

# Ensure no duplicate athletes with same name and date of birth
no_duplicate_name_birth = pa.Check(
    lambda df: ~df.duplicated(subset=["Name", "Born_Date"]),
    element_wise=False,
    error="Duplicate athlete records found with same Name and Born_Date."
)

# Logical validation for chronological consistency between Born and Died dates
date_logic = pa.Check(
    lambda df: (df["Born_Date"].isna() | df["Died_Date"].isna()) | (df["Died_Date"] >= df["Born_Date"]),
    element_wise=False,
    error="Died_Date earlier than Born_Date."
)

# Biometric sanity check: BMI must fall within realistic human range (15–45)
height_weight_logic = pa.Check(
    lambda df: (
        (df["Weight (kg)"] / ((df["Height (cm)"] / 100) ** 2)).between(15, 45)
    ),
    error="Unrealistic height-to-weight ratio."
)

# Bios Schema Definition

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


# Affiliations Pandera Checks

# Ensure unique affiliation clubs (avoid duplicates across countries/cities)
duplicate_affiliation_content_check = pa.Check(
    lambda df: ~df.duplicated(subset=["Affiliation_Club"]),
    element_wise=False,
    error="Duplicate affiliations found with different Affiliation_Ids (same club, city, and country)."
)

# Affiliations Schema Definition

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


# Results Pandera Checks

# Validate that medals are only awarded to top 3 positions
medal_position_logic_check = pa.Check(
    lambda df: (
        df["Medal"].isna() | df["Position"].isna() | df["Position"] < 3
    ),
    error="Medal assigned to invalid position (must be ≤ 3)."
)
null_strings = ['nan', '<NA>', 'N/A', 'NULL', 'None', '']

# Ensure that medal type corresponds correctly to athlete's position
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

# Results Schema Definition

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

# Edition Pandera Checks

game_types_list= ['Olympic Games', 'Intercalated Games', 'Youth Olympic Games','Forerunners to the Olympic Games']
edition_names_list=['Summer', 'Winter', 'Equestrian']

# Ensure unique games per year–edition–type combination
no_duplicate_games_check = pa.Check(
    lambda df: ~df.duplicated(subset=["Year", "Edition_Name", "Game_Type"]),
    element_wise=False,
    error="Duplicate game editions detected based on Year, Edition_Name, and Game_Type."
)

# Validate chronological consistency between Opened and Closed dates
edition_date_check = pa.Check(
    lambda df: (
        
        df["Opened"].isna() | df["Closed"].isna() | (df["Opened"] <= df["Closed"])
    ),
    
    error="Chronological order violated: check Opened, Closed edition dates."
)

# Validate chronological consistency between Competition start and end
Competition_date_check = pa.Check(
    lambda df: (

        
        df["Competition_Start"].isna() | df["Competition_End"].isna() | (df["Competition_Start"] <= df["Competition_End"])
        
    ),

    error="Chronological order violated:check Start, End Competition dates."
)


# Edition Schema Definition
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

    ],
    name="games_schema"
)




def get_error_df(df, original_df):
    """
    Process Pandera failure cases to create a structured error report.

    Parameters
    ----------
    df : pd.DataFrame
        Pandera failure cases (columns: ['failure_case', 'column', 'check'])
    original_df : pd.DataFrame
        Original dataset validated by Pandera.

    Returns
    -------
    pd.DataFrame
        Wide-format DataFrame combining failed checks and related record details.
    """

    # Identify columns most frequently associated with validation failures
    max_count = df['column'].value_counts().max()
    valid_columns = df['column'].value_counts()[df['column'].value_counts() == max_count].index
    filtered = df[df['column'].isin(valid_columns)]

    # Collect all unique failing checks for grouping
    all_checks = filtered['check'].unique()
    dfs = []

    # For each failed check, collect failed values by column
    for chk in all_checks:
        chk_filtered = filtered[filtered['check'] == chk]
        grouped = chk_filtered.groupby('column')['failure_case'].apply(list)
        temp_df = pd.DataFrame({col: vals for col, vals in grouped.items()})
        temp_df['failed_check'] = chk
        dfs.append(temp_df)

    # Concatenate all check-specific failure summaries
    wide_df = pd.concat(dfs, ignore_index=True)

    # Merge with original data to recover missing context columns
    missing_cols = [c for c in original_df.columns if c not in wide_df.columns and c != 'failed_check']
    merge_cols = [c for c in wide_df.columns if c != 'failed_check']
 
    final_df = pd.merge(
        wide_df,
        original_df[merge_cols + missing_cols].drop_duplicates(),
        on=merge_cols,
        how='left'
    )


    return final_df.drop_duplicates()


# MAIN DATA VALIDATION PIPELINE

def data_validation_quality_checks():
    """
    Load cleaned data from silver bucket, validate using Pandera schemas,
    and save any failure cases back to MinIO for auditability.
    """

    # S3/MinIO connection setup
    s3_endpoint = "http://minio:9000"
    access_key = "accesskey"
    secret_key = "secretkey"
    silver_bucket = "silver"

    s3fs_opts = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": s3_endpoint},
    }


    # Load pre-cleaned parquet datasets
    logger.info("Loading data from silver bucket...")

    bios_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data_II/cleaned_biodata.parquet", storage_options=s3fs_opts)
    results_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data/cleaned_results.parquet", storage_options=s3fs_opts)
    editions_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data_II/cleaned_editions.parquet", storage_options=s3fs_opts)
    affiliation_df = pd.read_parquet(f"s3://{silver_bucket}/clean_data/dim_affiliation.parquet", storage_options=s3fs_opts)

   # Validate bios
    try: 
        bios_schema.validate(bios_df, lazy=True) 
        logger.info("Bios validation PASSED!")
        bios_failed = False
    except pa.errors.SchemaErrors as exc: 
        logger.error("Bios validation FAILED!")
        bios_error_df = exc.failure_cases
        bios_failed = True


    # Validate affiliations
    try: 
        affiliations_schema.validate(affiliation_df, lazy=True) 
        logger.info("Affiliations validation PASSED!") 
        affiliations_failed = False
    except pa.errors.SchemaErrors as exc: 
        logger.error("Affiliations validation FAILED!")
        affiliations_error_df = exc.failure_cases
        affiliations_failed = True

    # Validate editions
    try:
        editions_schema.validate(editions_df, lazy=True)
        logger.info("Editions validation PASSED!")
        editions_failed = False
    except pa.errors.SchemaErrors as exc:
        logger.error("Editions validation FAILED!")
        editions_error_df = exc.failure_cases
        editions_failed = True

    # Validate results
    try:
        results_schema.validate(results_df, lazy=True)
        logger.info("Results validation PASSED!")
        results_failed = False
    except pa.errors.SchemaErrors as exc:
        logger.error("Results validation FAILED!")
        results_error_df = exc.failure_cases
        results_failed = True


    # Process Failure Cases
    if bios_failed:
        bios_error_df = bios_error_df[["failure_case", "column", 'check']].sort_index()
        bios_failure_cases_df = get_error_df(bios_error_df, bios_df)
        logger.info("Bios failure cases dataframe created.")

    if affiliations_failed:
        affiliations_error_df = affiliations_error_df[["failure_case", "column", 'check']].sort_index()
        affiliations_failure_cases_df = get_error_df(affiliations_error_df, affiliation_df)
        logger.info("Affiliations failure cases dataframe created.")

    if editions_failed:
        editions_error_df = editions_error_df[["failure_case", "column", 'check']].sort_index()
        editions_failure_cases_df = get_error_df(editions_error_df, editions_df)
        logger.info("Editions failure cases dataframe created.")

    if results_failed:
        results_error_df = results_error_df[["failure_case", "column", 'check']].sort_index()
        results_failure_cases_df = get_error_df(results_error_df, results_df)
        logger.info("Results failure cases dataframe created.")


    # Save Validation Results
    logger.info("Saving failure cases to silver bucket...")
    if bios_failed:
        bios_failure_cases_df.to_parquet(f"s3://{silver_bucket}/failure_cases/bios_failure_cases.parquet", index=False, storage_options=s3fs_opts)
    if affiliations_failed:
        affiliations_failure_cases_df.to_parquet(f"s3://{silver_bucket}/failure_cases/affiliations_failure_cases.parquet", index=False, storage_options=s3fs_opts)
    if editions_failed:
        editions_failure_cases_df.to_parquet(f"s3://{silver_bucket}/failure_cases/editions_failure_cases.parquet", index=False, storage_options=s3fs_opts)
    if results_failed:
        results_failure_cases_df.to_parquet(f"s3://{silver_bucket}/failure_cases/results_failure_cases.parquet", index=False, storage_options=s3fs_opts)

    logger.info("All validation failure cases successfully saved to silver bucket.")