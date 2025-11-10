import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import re
import time, random

from .logger import logger
from .athlete_scrape import normalizing_to_string

# Maximum number of retry attempts for network requests
retry_num=3

def game_table_df(session):
    """
    Scrapes all Olympic Games edition tables from Olympedia.org
    and returns them as a single consolidated DataFrame.

    Args:
        session (requests.Session): A requests session with headers configured.

    Returns:
        pd.DataFrame: Combined DataFrame containing all Olympic edition tables.
    """

    # Randomized delay to avoid rate limiting
    time.sleep(random.uniform(0.5, 1.5))

    url = "https://www.olympedia.org/editions"

    # Retry mechanism to handle transient network issues    
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
        raise ValueError(f"Status {response.status_code} for game editions scraping")
    
    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    tables = []
    current_h2 = None
    current_h3 = None

    # Iterate through HTML tags to capture table hierarchy
    for tag in soup.find_all(["h2", "h3", "table"]):
        if tag.name == "h2":
            current_h2 = tag.get_text(strip=True)
        elif tag.name == "h3":
            current_h3 = tag.get_text(strip=True)
        elif tag.name == "table":
            # Extract NOC codes from <img src=".../XXX.png">
            for img in tag.find_all("img"):
                src = img.get("src", "")
                match = re.search(r"/([A-Z]{3})\.png", src)
                if match:
                    noc = match.group(1)
                    img.replace_with(noc)
                else:
                    img.replace_with("")

            # Convert each HTML table to a DataFrame
            df = pd.read_html(StringIO(str(tag)))[0]
            df["Game_Type"] = current_h2
            df["Edition_Name"] = current_h3
            tables.append(df)

    # Combine all tables
    editions_df = pd.concat(tables, ignore_index=True)

    return editions_df





def scrap_editions():
    """
    Extracts Olympic Games edition data from Olympedia.org,
    normalizes it, and saves the raw dataset to the bronze MinIO bucket.

    Steps:
        1. Open a requests session with appropriate headers.
        2. Scrape and extract all editions tables using `game_table_df`.
        3. Normalize data types and text formatting.
        4. Save the raw data as a Parquet file to the MinIO bronze bucket.
    """

    # MinIO S3 configuration
    s3_endpoint = "http://minio:9000"  
    access_key = "accesskey"
    secret_key = "secretkey"
    bronze_bucket = "bronze"

    # S3FS-compatible storage options
    s3fs_opts = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": s3_endpoint},
    }

    # Initialize an HTTP session for stable and efficient requests
    with requests.Session() as session:
        session.headers.update({
            "User-Agent": "OlympediaScraper/1.0 (+your-email@gmail.com)"
        })
        
        df= game_table_df(session)
        df = normalizing_to_string(df)
        df.to_parquet(f"s3://{bronze_bucket}/raw_data/editions.parquet", index=False, storage_options=s3fs_opts)
       
        logger.info(f"Saved {len(df)} olympic editions rows with NOC codes extracted.")