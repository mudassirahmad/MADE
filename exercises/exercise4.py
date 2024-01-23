import urllib.request
import zipfile
import pandas as pd
import sqlalchemy

def download_and_extract_data(url, destination):
    urllib.request.urlretrieve(url, "data.zip")
    with zipfile.ZipFile('data.zip', 'r') as zip_ref:
        zip_ref.extractall(destination)

def convert_celsius_to_fahrenheit(temp_cels):
    return (temp_cels * 9/5) + 32

def validate_data(df):
    valid_gear_aktiv_values = ["Ja", "Nein"]

    df = df.loc[df['Geraet'] > 0]
    df = df.loc[(df['Monat'] >= 1) & (df['Monat'] <= 12)]
    df = df.loc[(df['Temperatur'] >= -459.67) & (df['Temperatur'] <= 212)]
    df = df.loc[(df['Batterietemperatur'] >= -459.67) & (df['Batterietemperatur'] <= 212)]
    df = df.loc[df['Geraet aktiv'].isin(valid_gear_aktiv_values)]

    return df

def save_to_database(df, table_name, database_url):
    engine = sqlalchemy.create_engine(database_url)
    
    df.to_sql(table_name, con=engine, if_exists='replace', index=False, dtype={
        "Geraet": sqlalchemy.BIGINT,
        "Hersteller": sqlalchemy.TEXT,
        "Model": sqlalchemy.TEXT,
        "Monat": sqlalchemy.BIGINT,
        "Temperatur": sqlalchemy.FLOAT,
        "Batterietemperatur": sqlalchemy.FLOAT,
        "Geraet aktiv": sqlalchemy.TEXT,
    })

if __name__ == "__main__":
    data_url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
    data_destination = "../exercises/data/"
    table_name = 'temperatures'
    database_url = 'sqlite:///temperatures.sqlite'

    download_and_extract_data(data_url, data_destination)

    fields = ["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"]
    df = pd.read_csv('../exercises/data/data.csv', sep=";", index_col=False, usecols=fields, encoding='utf-8', decimal=",")
    df.rename(columns={"Temperatur in °C (DWD)": "Temperatur", "Batterietemperatur in °C": "Batterietemperatur"}, inplace=True)

    df['Temperatur'] = convert_celsius_to_fahrenheit(df['Temperatur'])
    df['Batterietemperatur'] = convert_celsius_to_fahrenheit(df['Batterietemperatur'])

    df = validate_data(df)

    save_to_database(df, table_name, database_url)
