import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import re
import time, random

from logger import logger


retry_num=3


def game_table_df(session):

    time.sleep(random.uniform(0.5, 1.5))

    url = "https://www.olympedia.org/editions"

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
    
    soup = BeautifulSoup(response.content, "html.parser")

    tables = []
    current_h2 = None
    current_h3 = None

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

            # Convert table HTML to DataFrame
            df = pd.read_html(StringIO(str(tag)))[0]
            df["Game_Type"] = current_h2
            df["Edition_Name"] = current_h3
            tables.append(df)

    # Combine all tables
    editions_df = pd.concat(tables, ignore_index=True)

    # editions_df.to_csv("./raw_data/olympic_editions_with_noc.csv", index=False)
    # print(f"✅ Saved {len(editions_df)} rows with NOC codes extracted.")

    return editions_df




if __name__ == "__main__":

    with requests.Session() as session:
        session.headers.update({
            "User-Agent": "OlympediaScraper/1.0 (+your-email@gmail.com)"
        })
        df= game_table_df(session)

        df.to_csv("./raw_data/editions.csv", index=False)
        logger.info(f"Saved {len(df)} olympic editions rows with NOC codes extracted.")