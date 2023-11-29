import pandas as pd
import sqlalchemy

# Load CSV data from the direct link

url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
try:
    df = pd.read_csv(url, sep=';', decimal=',')
except:
    print(f'Couldn\'t extract csv from given url!)')

# Drop the "Status" column
df = df.drop(columns=['Status'])

df= df.dropna()


# Convert columns to appropriate data types
df['Laenge'] = pd.to_numeric(df['Laenge'], errors='coerce')
df['Breite'] = pd.to_numeric(df['Breite'], errors='coerce')
df['IFOPT'] = df['IFOPT'].astype(str)

# Filter rows based on the specified conditions
df = df[(df['Verkehr'].isin(['FV', 'RV', 'nur DPN']))]

df = df[(df['Laenge'].between(-90, 90, inclusive='both'))]

df = df[(df['Breite'].between(-90, 90, inclusive='both'))]

df = df[(df['IFOPT'].str.match(r'^[A-Za-z]{2}:\d+:\d+(\:\d+)?$'))]


# Create SQLite connection and write data to the database

df.to_sql('trainstops', 'sqlite:///trainstops.sqlite', if_exists='replace', index=False, dtype={
        "EVA_NR": sqlalchemy.BIGINT,
        "DS100": sqlalchemy.TEXT,
        "IFOPT": sqlalchemy.TEXT,
        "NAME": sqlalchemy.TEXT,
        "Verkehr": sqlalchemy.TEXT,
        "Laenge": sqlalchemy.FLOAT,
        "Breite": sqlalchemy.FLOAT,
        "Betreiber_Name": sqlalchemy.TEXT,
        "Betreiber_Nr": sqlalchemy.BIGINT
    })