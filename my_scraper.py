import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import StringIO


def biodata_table_df(soup, i):
    table = soup.find('table', {'class': 'biodata'})
    table = pd.read_html(StringIO(str(soup)), index_col=0, dtype_backend='pyarrow')[0]
    table= table.T
    table['athlete_id'] = i
    return table

def results_table_df(soup, i):
    table = soup.find("table", {"class": "table"})

    table_df= pd.read_html(StringIO(str(table)), dtype_backend='pyarrow')[0]

    table_df['NOC'] = None
    table_df['Discipline'] = None
    table_df['Athlete_id'] = i

    rows_with_meta = table_df.index[~table_df['Games'].isna()].tolist()

    table_df.loc[rows_with_meta, "NOC"] = table_df.loc[rows_with_meta, "NOC / Team"]
    table_df.loc[rows_with_meta, "Discipline"] = table_df.loc[rows_with_meta, "Discipline (Sport) / Event"]

    table_df.rename(columns={'NOC / Team': 'Team', 'Discipline (Sport) / Event': 'Event'}, inplace=True)

    column_to_ffill = ['NOC', 'Discipline', 'As', 'Games']
    table_df[column_to_ffill] = table_df[column_to_ffill].ffill()

    table_df.drop(columns=['Unnamed: 6'], errors='ignore', inplace=True)

    table_df = table_df.drop(rows_with_meta)

    return table_df


if __name__ == "__main__":

    size = 20

    base_url = "https://www.olympedia.org/athletes"



    bio_concate_df = pd.DataFrame()
    result_concate_df = pd.DataFrame()
    errors = []
    
    for i in range(1, size):

        if i % 5 == 0 and i != 0:
            bio_concate_df.to_csv(f"biodata_temp_{i}.csv", index=False)
            result_concate_df.to_csv(f"results_temp_{i}.csv", index=False)


        try:
            url = f"{base_url}/{i}"
            response = requests.get(url, timeout=60)

            if response.status_code == 200:
                    
                soup = BeautifulSoup(response.content, "html.parser")

        

                biodata = biodata_table_df(soup, i)
                result = results_table_df(soup, i)


                bio_concate_df= pd.concat([bio_concate_df, biodata], ignore_index=True)
                result_concate_df= pd.concat([result_concate_df, result], ignore_index=True)
                print(f"Scraped athlete ID: {i}")

            else:
                errors.append(i)
                print(f"Failed to retrieve data for athlete ID: {i}")

        except Exception as e:
            errors.append(i)
            print(f"Error occurred for athlete ID: {i}: {e}")

    bio_concate_df.to_csv("biodata.csv", index=False)
    result_concate_df.to_csv("results.csv",  index=False)

    with open("errors.txt", "w") as file:
        for error in errors:
            file.write(error + "\n")