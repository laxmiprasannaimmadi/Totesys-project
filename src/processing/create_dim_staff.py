import pandas as pd

def create_dim_staff(staff_tbl_df,dept_tbl_df):

    """
    Cleans and restructures the dataframes ready for the dimension table.

            Parameters:
                    Requires the staff and department dataframes.

            Returns:
                    Cleaned dataframe.
    """

    dim_staff_column_names = ['staff_id', 'first_name','last_name','email_address']
    dim_staff = staff_tbl_df.loc[:,dim_staff_column_names]
    dim_staff['department_name'] = dept_tbl_df['department_name']
    dim_staff['location'] = dept_tbl_df['location']
    mergedStuff = pd.merge(staff_tbl_df, dept_tbl_df, on=['department_id'],how='inner')
    df=mergedStuff.filter(['staff_id', 'first_name','last_name','email_address','department_name','location'])
    return df