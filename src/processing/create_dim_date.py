import pandas as pd
from datetime import datetime, date, time

def create_dim_date():

    """
    Creates the dim_date table. Extends from the beginning of the year until the nearby future.

            Parameters:
                    No inputs.

            Returns:
                    Date dimension as a dataframe.
    """

    date_range = pd.date_range(start="2022-01-01", end="2025-05-21", freq="D")
    dim_date = pd.DataFrame(date_range, columns=['date_id'])
    dim_date['date_id'] = pd.to_datetime(dim_date['date_id'])
    dim_date['year'] = dim_date['date_id'].dt.year
    dim_date['month'] = dim_date['date_id'].dt.month
    dim_date['day'] = dim_date['date_id'].dt.day
    dim_date['day_of_week'] = dim_date['date_id'].dt.weekday
    dim_date['day_name'] = dim_date['date_id'].dt.strftime('%A')
    dim_date['month_name'] = dim_date['date_id'].dt.strftime('%B')
    dim_date['quarter'] = dim_date['date_id'].dt.quarter
    return dim_date
