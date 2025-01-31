import os
import pandas as pd
import pipeline
import time

# Test data URLs
TOURIST_DATA_URL = "https://res.cloudinary.com/dx6obccn6/raw/upload/v1701604753/tourists-data_aas2zo.csv"
TOURIST_STATES_URL = "https://res.cloudinary.com/dx6obccn6/raw/upload/v1704827971/tourist_areas_with_states_zwmucy.csv"
TRANSPORTATION_DATA_URL = "https://res.cloudinary.com/dx6obccn6/raw/upload/v1701604752/transportation_vpbfi6.csv"


#check if the data sets are being fetched from the sources
def test_fetch_and_transform_dataset_1():
    df = pipeline.fetch_and_transform_dataset_1(TOURIST_DATA_URL)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty

    df_2 = pipeline.fetch_and_transform_dataset_2(TRANSPORTATION_DATA_URL)
    assert isinstance(df_2, pd.DataFrame)
    assert not df_2.empty
    

def test_merge_data():
    tourist_df = pipeline.fetch_and_transform_dataset_1(TOURIST_DATA_URL)
    states_df = pd.read_csv(TOURIST_STATES_URL, encoding='latin1', sep=",")
    merged_df = pipeline.merge_data(tourist_df, states_df)

    # Check if the required column exists in the merged DataFrame
    required_column = 'State(s)'
    assert required_column in merged_df.columns, f"Column '{required_column}' not found in merged DataFrame"
    
    assert isinstance(merged_df, pd.DataFrame)
    assert not merged_df.empty


# test if the dataframes are store in the Database and that file is created or not
def test_store_dataframes():

    # Store the original database paths for later restoration
    org_db_file1 = pipeline.db_file1
    try:
        # Use temporary paths for testing
        temp_db_file = "../data/data_test.sqlite"

        tourist_df = pipeline.fetch_and_transform_dataset_1(TOURIST_DATA_URL)
        transport_df = pipeline.fetch_and_transform_dataset_2(TRANSPORTATION_DATA_URL)
        states = pipeline.fetch_dataset_1_1(TOURIST_STATES_URL)
        tourist_data_merged = pipeline.merge_data(tourist_df, states)

        # Temporarily change the database paths for the test
        pipeline.db_file1 = temp_db_file

        # Call the original function
        pipeline.store_dataframes(tourist_data_merged, transport_df)
        assert os.path.exists(temp_db_file)
        
    
    finally:
        # Restore the original database paths
        pipeline.db_file1 = org_db_file1

        time.sleep(5)

        # Clean up: Remove temporary files
        os.remove(temp_db_file)



def test_main():
    #test  the whole pipeline
    pipeline.main()

