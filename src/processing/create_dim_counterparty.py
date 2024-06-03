import pandas as pd

def create_dim_counterparty(address_df, counterparty_df):

    """
    Cleans and restructures the dataframes ready for the dimension table.

            Parameters:
                    Requires the address and counterparty dataframes.

            Returns:
                    Cleaned dataframe.
    """

    renamed_counterparty_df = counterparty_df.rename(columns={
        "legal_address_id":"address_id"
        })
    renamed_address_df = address_df.rename(columns={
        "address_line_1":"counterparty_legal_address_line_1",
        "address_line_2":"counterparty_legal_address_line_2",
        "district":"counterparty_legal_district",
        "city":"counterparty_legal_city",
        "postal_code":"counterparty_legal_postal_code",
        "country":"counterparty_legal_country",
        "phone":"counterparty_legal_phone_number"
        })
    big_df = pd.merge(renamed_address_df, renamed_counterparty_df, on="address_id")
    required_columns = [
        'counterparty_id',
        'counterparty_legal_name',
        'counterparty_legal_address_line_1',
        'counterparty_legal_address_line_2',
        'counterparty_legal_district',
        'counterparty_legal_city',
        'counterparty_legal_postal_code',
        'counterparty_legal_country',
        'counterparty_legal_phone_number'
        ]
    dim_df = big_df.filter(required_columns)
    return dim_df