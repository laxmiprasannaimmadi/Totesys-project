import pandas as pd
import json

def grab_currency_conversion_info():

    """
    Grabs currency information from a JSON, designed to match a currrency code with it's
    respective full name.

            Parameters:
                    No parameters taken.

            Returns:
                    Dictionary of currencies with the key as currency code and the value
                    as it's respective name.
    """

    try:
        with open("src/processing/currency_information_data.json", "r") as file:
            currency_data = json.load(file)
            return currency_data
    except FileNotFoundError:
        with open("currency_information_data.json", "r") as file:
            currency_data = json.load(file)
            return currency_data
        
def create_dim_currency(df):

    """
    Cleans and restructures the dataframe ready for the dimension table. References external
    currency information in currency_information_data.json to find key values.

            Parameters:
                    Requires the currency dataframe.

            Returns:
                    Cleaned dataframe.
    """
    
    currency_dict = grab_currency_conversion_info()
    
    currency_info_df = pd.DataFrame(list(currency_dict['currencies'].items()), columns=['currency_code', 'currency_name'])

    clean_df = df.drop(columns=["created_at", "last_updated"])

    merged_df = clean_df.merge(currency_info_df, on='currency_code', how='left')
    
    return merged_df
    