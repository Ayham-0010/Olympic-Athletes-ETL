import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from io import StringIO
import time, random, glob, os
import s3fs
from concurrent.futures import ThreadPoolExecutor, as_completed

from .logger import logger



# CONFIGURATION 

# Total number of athlete pages to scrape
size = 150000

# Maximum number of retries per request
retry_num = 3

# Maximum number of concurrent threads
max_threads = 10

# Save progress after scraping every N athletes
checkpoint_every = 1000

# S3 / MinIO configuration
s3_endpoint = "http://minio:9000"
access_key = "accesskey"
secret_key = "secretkey"
bronze_bucket = "bronze"

# s3fs connection options
s3fs_opts = {
    "key": access_key,
    "secret": secret_key,
    "client_kwargs": {"endpoint_url": s3_endpoint},
}



# UTILITY FUNCTIONS

def normalizing_to_string(df):
    """
    Normalize object columns in a DataFrame to Pandas StringDtype.

    This ensures schema consistency across checkpoints and final saves.
    """

    df = df.copy()
    for col in df.columns:
        if df[col].dtype == 'object':

            df[col] = df[col].replace({np.nan: pd.NA, None: pd.NA})

            df[col] = df[col].astype("string")
    return df

def get_latest_checkpoint():
    """
    Find the latest checkpoint files for both biodata and results.

    Returns:
        tuple: (latest_biodata_checkpoint_num, latest_results_checkpoint_num)
    """

    fs = s3fs.S3FileSystem(**s3fs_opts)
    folder = f"{bronze_bucket}/checkpoints"

    
    if not fs.exists(folder):
        fs.mkdirs(folder, exist_ok=True)
        return None, None

    def find_latest(prefix):
        """Return the highest numeric suffix for checkpoint files with given prefix."""    
        
        files = fs.glob(f"{folder}/{prefix}*.parquet")
        numbers = []
        for f in files:
            try:
                
                filename = os.path.basename(f)
                num = int(os.path.splitext(filename)[0].split("_")[-1])
                numbers.append(num)
            except ValueError:
                continue
        return max(numbers) if numbers else None

    latest_bio = find_latest("biodata_temp_")
    latest_results = find_latest("results_temp_")

    return latest_bio, latest_results


def biodata_table_df(soup, i):
    """
    Extract and parse the biodata table from an athlete's page.

    Args:
        soup (BeautifulSoup): Parsed HTML of athlete page.
        i (int): Athlete ID.

    Returns:
        pd.DataFrame: Normalized biodata for the athlete.
    """

    table = soup.find('table', {'class': 'biodata'})
    if not table:
        raise ValueError(f"No biodata table for athlete {i}")
    df = pd.read_html(StringIO(str(table)), index_col=0, dtype_backend='pyarrow')[0]
    df = df.T
    df['Athlete_Id'] = i
    return df


def results_table_df(soup, i):
    """
    Extract and parse the results table from an athlete's page.

    Args:
        soup (BeautifulSoup): Parsed HTML of athlete page.
        i (int): Athlete ID.

    Returns:
        pd.DataFrame: Normalized competition results for the athlete.
    """

    table = soup.find("table", {"class": "table"})

    if not table:
        raise ValueError(f"No results table for athlete {i}")
    
    df= pd.read_html(StringIO(str(table)), dtype_backend='pyarrow')[0]

    df['NOC'] = None
    df['Discipline'] = None
    df['Athlete_Id'] = i

    # Identify rows containing metadata for subsequent results
    rows_with_meta = df.index[~df['Games'].isna()].tolist()

    # Extract and forward-fill metadata
    df.loc[rows_with_meta, "NOC"] = df.loc[rows_with_meta, "NOC / Team"]
    df.loc[rows_with_meta, "Discipline"] = df.loc[rows_with_meta, "Discipline (Sport) / Event"]

    df.rename(columns={'NOC / Team': 'Team', 'Discipline (Sport) / Event': 'Event'}, inplace=True)

    column_to_ffill = ['NOC', 'Discipline', 'As', 'Games']
    df[column_to_ffill] = df[column_to_ffill].ffill()

    df.drop(columns=['Unnamed: 6'], errors='ignore', inplace=True)

    df = df.drop(rows_with_meta)

    return df


def scrape_athlete(i, session):
    """
    Scrape data for a single athlete (biodata + results).

    Args:
        i (int): Athlete ID.
        session (requests.Session): Persistent session for HTTP requests.

    Returns:
        tuple: (athlete_id, biodata_df, results_df)
    """

    # Randomized delay to avoid rate limiting
    time.sleep(random.uniform(0.5, 1.5))

    base_url = "https://www.olympedia.org/athletes"
    url = f"{base_url}/{i}"
    

    for attempt in range(retry_num):
        try:
            response = session.get(url, timeout=60)
            if response.status_code == 200:
                break
        except requests.RequestException:
            if attempt == retry_num-1:
                raise
            time.sleep(2)


    if response.status_code != 200:
        raise ValueError(f"Status {response.status_code} for athlete {i}")

    soup = BeautifulSoup(response.content, "html.parser")

    biodata = biodata_table_df(soup, i)
    results = results_table_df(soup, i)

    return (i, biodata, results)




# MAIN SCRAPING FUNCTION

def scrap_athletes():
    """
    Main pipeline to scrape all athlete biodata and results.

    Features:
        - Automatic resume from last checkpoint
        - Multithreaded scraping for efficiency
        - Regular checkpoint saving to S3
        - Error logging for failed athletes
    """



    # Resume logic (checkpointing) 

    bio_ckpt, res_ckpt = get_latest_checkpoint()
    latest = min(bio_ckpt or 0, res_ckpt or 0)

    if latest:

        # Load latest checkpoints   
        biodata_checkpoint = pd.read_parquet(f"s3://{bronze_bucket}/checkpoints/biodata_temp_{latest}.parquet", storage_options=s3fs_opts)
        results_checkpoint = pd.read_parquet(f"s3://{bronze_bucket}/checkpoints/results_temp_{latest}.parquet", storage_options=s3fs_opts)
    
        
        scraped_ids = set(biodata_checkpoint["Athlete_Id"].tolist())
        bio_list = [biodata_checkpoint]
        result_list = [results_checkpoint]
        
        logger.info(f"Resuming from checkpoint #{latest}")

    else:
        scraped_ids = set()
        bio_list = []
        result_list = []

        logger.info("No checkpoint found, starting fresh.")

    # Determine remaining athlete IDs
    athlete_ids = range(1, size)
    remaining_ids = [i for i in athlete_ids if i not in scraped_ids]

    logger.info(f"Remaining athletes to scrape: {len(remaining_ids)}")

    # Multithreaded scraping setup
    errors = []
    completed = len(scraped_ids)

    with requests.Session() as session:
        session.headers.update({
            "User-Agent": "OlympediaScraper/1.0 (+your-email@gmail.com)"
        })

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = {executor.submit(scrape_athlete, i, session): i for i in remaining_ids}
            
            for future in as_completed(futures):
                i = futures[future]
                try:
                    i, biodata, results = future.result()
                    bio_list.append(biodata)
                    result_list.append(results)
                    completed += 1
                    # logger.info(f"Scraped athlete {i}")


                    # Save checkpoint every N athletes
                    if completed % checkpoint_every == 0:
                        bio_concate_df = pd.concat(bio_list, ignore_index=True)
                        result_concate_df = pd.concat(result_list, ignore_index=True)

                        bio_concate_df= normalizing_to_string(bio_concate_df)
                        result_concate_df= normalizing_to_string(result_concate_df)

                        bio_concate_df.to_parquet(f"s3://{bronze_bucket}/checkpoints/biodata_temp_{completed}.parquet", index=False, storage_options=s3fs_opts)
                        result_concate_df.to_parquet(f"s3://{bronze_bucket}/checkpoints/results_temp_{completed}.parquet", index=False, storage_options=s3fs_opts)


                        logger.info(f"Checkpoint saved at {completed} athletes")
                

                except Exception as e:
                    errors.append((i, str(e)))
                    logger.error(f"Error scraping athlete {i}: {e}")



    # Final aggregation and save
    bio_concate_df = pd.concat(bio_list, ignore_index=True)
    result_concate_df = pd.concat(result_list, ignore_index=True)

    bio_concate_df = normalizing_to_string(bio_concate_df)
    result_concate_df = normalizing_to_string(result_concate_df)

    bio_concate_df.to_parquet(f"s3://{bronze_bucket}/raw_data/biodata.parquet", index=False, storage_options=s3fs_opts)
    result_concate_df.to_parquet(f"s3://{bronze_bucket}/raw_data/results.parquet", index=False, storage_options=s3fs_opts)

    logger.info("Saving final data files.")


    if errors:
        # errors is now a list of tuples: (athlete_id, exception)
        failed_athletes_df = pd.DataFrame(errors, columns=["failed_athlete_id", "error_message"])
        
        # Convert error_message to string for consistent Parquet schema
        failed_athletes_df["error_message"] = failed_athletes_df["error_message"].astype(str)
        
        failed_athletes_df.to_parquet(f"s3://{bronze_bucket}/scrape_failures/failed_athletes.parquet",index=False,storage_options=s3fs_opts)
        
        logger.info(f"Saved {len(errors)} failed athlete IDs with error details to Bronze layer.")
    else:
        logger.info("No scraping errors - all athletes processed successfully.")
        
  
    logger.info("Scraping completed.")