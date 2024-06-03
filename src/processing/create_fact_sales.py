import pandas as pd
from datetime import datetime, date, time
from decimal import Decimal

def create_fact_sales(sales_order_df):

    """
    Cleans and restructures the dataframe ready for the fact table.

            Parameters:
                    Requires the sales order dataframes.

            Returns:
                    Cleaned dataframe.
    """

    copied_sales_order = sales_order_df.copy(deep=True)
    renamed_sales_order = copied_sales_order.rename(columns={"staff_id":"sales_staff_id"})
    renamed_sales_order['created_date'] = renamed_sales_order['created_at'].apply(
        lambda x: datetime.fromisoformat(x).date()
        )
    renamed_sales_order['created_time'] = renamed_sales_order['created_at'].apply(
        lambda x: datetime.fromisoformat(x).time()
        )
    renamed_sales_order['last_updated_date'] = renamed_sales_order['last_updated'].apply(
        lambda x: datetime.fromisoformat(x).date()
        )
    renamed_sales_order['last_updated_time'] = renamed_sales_order['last_updated'].apply(
        lambda x: datetime.fromisoformat(x).time()
        )
    renamed_sales_order['agreed_delivery_date'] = renamed_sales_order['agreed_delivery_date'].apply(
        lambda x: datetime.fromisoformat(x).date()
        )
    renamed_sales_order['agreed_payment_date'] = renamed_sales_order['agreed_payment_date'].apply(
        lambda x: datetime.fromisoformat(x).date()
        )
    renamed_sales_order['unit_price'] = renamed_sales_order['unit_price'].apply(
        pd.to_numeric, errors= 'coerce'
        )
    filtered_df = renamed_sales_order.drop(columns = ['created_at','last_updated'])
    result_df = filtered_df.reindex([
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id",
    ], axis = 1)
    return result_df