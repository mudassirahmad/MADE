import pandas as pd
from sqlalchemy import create_engine


tourist_data_url="https://res.cloudinary.com/dx6obccn6/raw/upload/v1701604753/tourists-data_aas2zo.csv"
tourist_states_url="https://res.cloudinary.com/dx6obccn6/raw/upload/v1704621759/tourist_areas_with_states_ntknb0.csv"
transportation_data_url="https://res.cloudinary.com/dx6obccn6/raw/upload/v1701604752/transportation_vpbfi6.csv"


# SQLite database file
db_file1 = '../data/data.sqlite'
#db_file2 = '../data/transport.sqlite'


def fetch_and_transform_dataset_1(data):
    #loads dataset 2 from URL and returns the cleaned dataframe
    df = pd.read_csv(data, encoding='latin1', sep=";", skiprows=10)

    column_names = ['Year', 'Train', 'Land', 'domestic arrival', 'domestic overnight stays',
                    'domestic avg stay (days)', 'int. arrival', 'intl. overnight stays',
                    'intl. avg stay (days)', 'total arrival', 'total overnight stays',
                    'total avg stay (days)']
    #rename the columns
    df.columns=column_names
    df = df.iloc[:-4].reset_index(drop=True)
    
    #dropping empty rows
    df.replace('-', pd.NA, inplace=True)
    df.replace('.', pd.NA, inplace=True)
    clean_df = df.dropna()
    #df.to_csv('tourist_data.csv', index=False, encoding='utf-8-sig')
    return clean_df
    

def fetch_dataset_1_1(data):
    return pd.read_csv(data, encoding='latin1', sep=",")


def fetch_and_transform_dataset_2(data):
    #loads dataset 2 from URL and returns the cleaned dataframe
    df = pd.read_csv(data, encoding='latin1', sep=";", skiprows=7)
    column_names=['Year', 'Company Type', 'State(s)','Companies', 'Person Transported', 
                  'Transportation Per KM', 'Mileage', 'Transportation Offer', 
                  'Revenue from Local Transport (EUR)']
    
    #ensuring LongIntegerColumn are displayed as full integers
    pd.set_option('display.float_format', lambda x: '{:.0f}'.format(x))

    #rename the columns
    df.columns=column_names
    df = df.iloc[:-3].reset_index(drop=True)  

    return df 


def merge_data(tourist,states):
    #process and clean mergeed tourist data
    tourist_data=(pd.merge(tourist,states,left_on="Land",right_on="Tourist Area", how='left'))

    missing_values = {
        'Oberes Maintal - Coburger Land (bis 2016)': 'Baden-Württemberg',
        'Nördlicher Schwarzwald': 'Bavaria',
    }

    #rows_with_nan=tourist_data[tourist_data['State(s)'].isna()]

    # Iterate through the custom values and fill missing 'States' accordingly
    for land, state in missing_values.items():
        tourist_data.loc[tourist_data['Land'] == land, 'State(s)'] = state

    tourist_data=tourist_data.drop('Tourist Area', axis=1)
    #tourist_data.to_csv('tourist_data.csv', index=False, encoding='utf-8-sig')
    return tourist_data

def store_dataframes(tourist_df, transport_df):
    #push the data sets to the data folder
    engine1= create_engine(f'sqlite:///{db_file1}')
    #engine2= create_engine(f'sqlite:///{db_file2}')
    tourist_df.to_sql('Tourist', engine1, if_exists="replace")
    transport_df.to_sql('Transport', engine1, if_exists="replace")
    engine1.dispose()
    #engine2.dispose()

def main():
    tourist = fetch_and_transform_dataset_1(tourist_data_url)
    transportation_data=fetch_and_transform_dataset_2(transportation_data_url)
    states=fetch_dataset_1_1(tourist_states_url)
    tourist_data=merge_data(tourist,states)
    store_dataframes(tourist_data,transportation_data)

if __name__ == "__main__":
    main()