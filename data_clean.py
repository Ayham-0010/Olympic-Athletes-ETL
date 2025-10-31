import pandas as pd
import re


# Bio data cleaning

df = pd.read_csv("../raw_data/biodata.csv")

df["Name"]= df["Used name"].str.replace('•',' ', regex=False)

df['Hight (cm)'] = pd.to_numeric(df['Measurements'].str.split('/').str[0].str.strip(' cm'), errors='coerce')
df['Weight (kg)'] =pd.to_numeric(df['Measurements'].str.split('/').str[1].str.strip(' kg'), errors='coerce')

date_pattern = r'(\d{1,2}\s+[A-Za-z]+\s+\d{4}|\d{4})'

df['Born_Date'] = df['Born'].str.extract(date_pattern)
df['Born_Date'] = pd.to_datetime(df['Born_Date'], format="mixed", errors='coerce')

df['Died_Date'] = df['Born'].str.extract(date_pattern)
df['Died_Date'] = pd.to_datetime(df['Died_Date'], format="mixed", errors='coerce')

location_pattern = r"in\s+(.+?),\s*([A-Za-zÀ-ÿ'’\- ]+)\s*\((\w+)\)$"

df[['Born_City', 'Born_Region', 'Born_Country']] = df['Born'].str.extract(location_pattern, expand=True)

df[['Affiliations_Club', 'Affiliations_City', 'Affiliations_Country']] = (
    df['Affiliations']
    .astype(str)
    .str.strip()
    .str.extract(r'^(.+?)(?:,\s*(.+?))?(?:\s*\((.+?)\))?$')
)

df['Roles'] = df['Roles'].astype(str).str.strip().replace('—', '').str.split(' • ')

columns_to_drop = [ 'Used name', 'Born', 'Died', 'Full name', 'Measurements', 'Affiliations','Title(s)',  'Nationality', 'Other names', 'Original name', 'Name order', 'Nick/petnames']

df = df.drop(columns=columns_to_drop)

# df.to_csv('./clean_data/cleaned_biodata.csv', index=False)


# Results data cleaning

df = pd.read_csv("../raw_data/results.csv")

df[['Game Year', 'Game Type']] = df['Games'].str.extract(r'^(\d{4})\s+(.*)$', expand=True)
df['Game Year'] = pd.to_numeric(df['Game Year'])

df['Position'] = df['Pos'].str.extract(r'(\d+)')
df['Position'] = pd.to_numeric(df['Position'])

df['Tied'] = df['Pos'].str.contains('=')

columns_to_drop = ['Nationality', 'Unnamed: 7', 'Games', 'Pos']
df = df.drop(columns=columns_to_drop)

# df.to_csv('./clean_data/cleaned_results.csv', index=False)


# Editions data cleaning

df = pd.read_csv("../raw_data/editions.csv")

df = df[df['Game_Type'] != 'Ancient Olympic Games']
df.drop(columns = '#', inplace=True)
df.rename(columns={'Unnamed: 7':'Comments'}, inplace=True)



def normalize_competition(text):
    if pd.isna(text):
        return text

    text = text.replace("–", "-")
    # Fix cases like "6-13 April" → "6 April - 13 April"
    match = re.match(r"(\d+)\s*-\s*(\d+\s+[A-Za-z]+)", text)
    if match:
        day_start, day_end_part = match.groups()
        month = re.search(r"[A-Za-z]+", day_end_part).group(0)
        return f"{day_start} {month} - {day_end_part}"
    return text

df["Competition"] = df["Competition"].apply(normalize_competition)


df[["Competition_Start", "Competition_End"]] = (
    df["Competition"]
    .str.split("-", n=1, expand=True)  
    .apply(lambda x: x.str.strip())
)

date_cols = ["Opened", "Closed", "Competition_Start", "Competition_End"]
for col in date_cols:
    df[col] = pd.to_datetime(
        df[col] + " " + df["Year"].astype(str),
        format="%d %B %Y",
        errors="coerce"
    ).dt.strftime("%Y-%m-%d")

df = df.drop(columns='Competition')

# df.to_csv('./clean_data/cleaned_editions.csv', index=False)