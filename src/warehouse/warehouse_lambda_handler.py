from warehouse_utils import (
                             get_aws_secrets,
                             close_connection, 
                             create_dataframe_dictionaries, 
                             connect_to_db_engine, 
                             run_engine_to_insert_database,
                             delete_rows_from_warehouse)
import logging


logger = logging.getLogger("Ingestion Lambda Log")
logging.basicConfig()
logger.setLevel(logging.INFO)

def warehouse_lambda_handler(event={}, context=[]):
    logger.info("Running warehouse lambda handler...")

    table_list = ['dim_date', 'dim_design', 'dim_currency', 'dim_counterparty', 'dim_staff', 'dim_location', 'fact_sales_order']
    # table_list = ['fact_sales_order']

    df_dict = create_dataframe_dictionaries(table_list)
    # print(df_dict)
    secrets = get_aws_secrets()
    engine = connect_to_db_engine(secrets=secrets)
    delete_rows_from_warehouse(engine=engine)
    run_engine_to_insert_database(engine=engine, input_dict=df_dict)

    close_connection(engine)
    logger.info("Loaded data from s3 into warehouse successfully")