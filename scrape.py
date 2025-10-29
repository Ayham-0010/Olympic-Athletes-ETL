import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO
import time, random, glob, os
from concurrent.futures import ThreadPoolExecutor, as_completed

from logger import logger


#  Configuration 

size = 100
retry_num = 3
max_threads = 3
checkpoint_every = 10

os.makedirs("./checkpoints", exist_ok=True)
os.makedirs("./raw_data", exist_ok=True)

def get_latest_checkpoint(folder="./checkpoints"):
    """
    Finds the latest checkpoint numbers for biodata and results files.
    Returns a tuple: (latest_bio, latest_results)
    Each value may be None if no matching files exist.
    """

    if not os.path.exists(folder):
        os.makedirs(folder)
        return None, None

    def find_latest(prefix):
        files = glob.glob(os.path.join(folder, f"{prefix}*.csv"))
        numbers = []
        for f in files:
            try:
                num = int(os.path.splitext(os.path.basename(f))[0].split("_")[-1])
                numbers.append(num)
            except ValueError:
                continue
        return max(numbers) if numbers else None

    latest_bio = find_latest("biodata_temp_")
    latest_results = find_latest("results_temp_")

    return latest_bio, latest_results

def biodata_table_df(soup, i):
    table = soup.find('table', {'class': 'biodata'})
    if not table:
        raise ValueError(f"No biodata table for athlete {i}")
    df = pd.read_html(StringIO(str(table)), index_col=0, dtype_backend='pyarrow')[0]
    df = df.T
    df['Athlete_Id'] = i
    return df

def results_table_df(soup, i):
    table = soup.find("table", {"class": "table"})

    if not table:
        raise ValueError(f"No results table for athlete {i}")
    
    df= pd.read_html(StringIO(str(table)), dtype_backend='pyarrow')[0]

    df['NOC'] = None
    df['Discipline'] = None
    df['Athlete_Id'] = i

    rows_with_meta = df.index[~df['Games'].isna()].tolist()

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
    Fetches and parses biodata and results for a single athlete ID.
    Returns (i, biodata_df, results_df) or raises an exception if something goes wrong.
    """
    
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





if __name__ == "__main__":


    #  Resume logic 

    bio_ckpt, res_ckpt = get_latest_checkpoint()
    latest = min(bio_ckpt or 0, res_ckpt or 0)

    if latest:
        biodata_checkpoint = pd.read_csv(f"./checkpoints/biodata_temp_{latest}.csv")
        results_checkpoint = pd.read_csv(f"./checkpoints/results_temp_{latest}.csv")
        scraped_ids = set(biodata_checkpoint["Athlete_Id"].tolist())
        bio_list = [biodata_checkpoint]
        result_list = [results_checkpoint]
        
        logger.info(f"Resuming from checkpoint #{latest}")

    else:
        scraped_ids = set()
        bio_list = []
        result_list = []

        logger.info("No checkpoint found, starting fresh.")

    #  Determine remaining IDs 
    athlete_ids = range(1, size)
    remaining_ids = [i for i in athlete_ids if i not in scraped_ids]

    logger.info(f"Remaining athletes to scrape: {len(remaining_ids)}")

    #  Multithreaded scraping with checkpoints 
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


                    # Checkpoint every N new results
                    if completed % checkpoint_every == 0:
                        bio_concate_df = pd.concat(bio_list, ignore_index=True)
                        result_concate_df = pd.concat(result_list, ignore_index=True)
                        bio_concate_df.to_csv(f"./checkpoints/biodata_temp_{completed}.csv", index=False)
                        result_concate_df.to_csv(f"./checkpoints/results_temp_{completed}.csv", index=False)
                        

                        logger.info(f"Checkpoint saved at {completed} athletes")
                

                except Exception as e:
                    errors.append(i)
                    logger.error(f"Error scraping athlete {i}: {e}")



    #  Final save 
    bio_concate_df = pd.concat(bio_list, ignore_index=True)
    result_concate_df = pd.concat(result_list, ignore_index=True)

    bio_concate_df.to_csv("./raw_data/biodata.csv", index=False)
    result_concate_df.to_csv("./raw_data/results.csv", index=False)

    logger.info("Saving final data files.")

    with open("errors.txt", "w") as file:
        for err in errors:
            file.write(str(err) + "\n")

    logger.info(f"{len(errors)} Errors logged to errors.txt.")
    logger.info("Scraping completed.")