import pandas as pd
import json

def create_dim_location(address_tbl_df):

    """
    Cleans and restructures the dataframe ready for the dimension table.

            Parameters:
                    Requires the address dataframe.

            Returns:
                    Cleaned dataframe.
    """

    modified_dim_location_df = address_tbl_df.rename(columns={'address_id': 'location_id'})

    modified_dim_location_df.pop('created_at')
    modified_dim_location_df.pop('last_updated')

    return modified_dim_location_df
