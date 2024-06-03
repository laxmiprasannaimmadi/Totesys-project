import json
import awswrangler as wr
from botocore.exceptions import ClientError
from botocore.session import get_session
import pg8000
from sqlalchemy import create_engine, delete, Table, MetaData
    
def create_dataframe_dictionaries(table_list):

    """
        Creates a dictionary of dataframes with the key as the name and the value as the dataframe.

                Parameters:
                        table_list: table names to gather data for in the dictionary.

                Returns:
                        A dictionary of dataframes requested.
    """
    
    df_dict = {}
    for table in table_list:
        key_data_df = wr.s3.read_parquet(path=f's3://nc-team-reveries-processing/{table}/')
        key_data_df.drop_duplicates(inplace=True)
        df_dict[table] = key_data_df
    return df_dict
    
def get_aws_secrets():
        
        """
        Gathers secrets from AWS for a database connection.

                Parameters:
                        No inputs are taken for this function.

                Returns:
                        A dictionary of database credentials.
        """
        
        secret_name = "team_reveries_warehouse"
        region_name = "eu-west-2"
        session = get_session()
        client = session.create_client("secretsmanager", region_name=region_name)
        secret_list = {}
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            secret = get_secret_value_response["SecretString"]
            secret_value = json.loads(secret)
            secret_list['username'] = secret_value["username"]
            secret_list['password'] = secret_value["password"]
            secret_list['database'] = secret_value["dbname"]
            secret_list['host'] = secret_value["host"]
            secret_list['port'] = secret_value["port"]
            return secret_list
        except ClientError:
            raise ClientError("Failure to return secrets from AWS")
            
def connect_to_db_engine(secrets):

    """
    Uses dictionary of secrets to create a PSQL database connection.

            Parameters:
                    secrets: a dictionary of secrets.

            Returns:
                    Connection to database.
    """
    
    try:
        engine = create_engine(f"postgresql+pg8000://{secrets['username']}:{secrets['password']}@{secrets['host']}:{secrets['port']}/{secrets['database']}")
        return engine
    except ConnectionError:
        raise ConnectionError("Failed to open DB connection")
    
def run_engine_to_insert_database(engine, input_dict):

    """
        Appends data rows from dataframes if the table is present in the database.

                Parameters:
                        engine: connection to the database using a sqlalchemy/pg8000 engine.
                        input_dict: dictionary of dataframe names as a key and the dataframes as the value.

                Returns:
                        Success message when completed.
    """
    
    with engine.begin() as connection:
            for dataframe_name, dataframe in input_dict.items():
                dataframe.to_sql(name=dataframe_name, con=connection, if_exists='append', index=False)
                success_message = "Succesfully moved dataframe rows to SQL database"
            return success_message

def delete_rows_from_warehouse(engine):

    """
        Deletes existing rows from warehouse.

                Parameters:
                        engine: connection to the database using a sqlalchemy/pg8000 engine.

                Returns:
                        No output.
    """
    
    tables = [
        'fact_sales_order',
        'dim_date', 
        'dim_design', 
        'dim_currency', 
        'dim_counterparty', 
        'dim_staff', 
        'dim_location'
    ]
    for table in tables:
        with engine.begin() as connection:
            metadata = MetaData()
            my_table = Table(table, metadata)
            connection.execute(my_table.delete())

def close_connection(conn):

    """
    Closes connection to the PSQL database

            Parameters:
                    conn: connection to sqlalchemy/pg8000 engine

            Returns:
                    No output.
    """

    try:
        conn.dispose()
    except ConnectionError:
        raise ConnectionError("Failed to close DB connection")
    
