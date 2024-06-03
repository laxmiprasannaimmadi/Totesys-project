try:
        from src.processing.create_dim_date import create_dim_date
        from src.processing.create_dim_counterparty import create_dim_counterparty
        from src.processing.create_dim_location import create_dim_location
        from src.processing.create_dim_staff import create_dim_staff
        from src.processing.create_dim_design import create_dim_design
        from src.processing.create_fact_sales import create_fact_sales
        from src.processing.create_dim_currency import create_dim_currency
        from src.processing.processing_utils import df_to_parquet, init_s3_client,  write_timestamp_to_s3, \
                        read_timestamp_from_s3, write_parquet_file_to_s3,filter_files_by_timestamp, \
                        list_objects_in_bucket
        from src.processing.s3_file_reader import s3_reader_filtered
except ModuleNotFoundError:
        from create_dim_date import create_dim_date
        from create_dim_counterparty import create_dim_counterparty
        from create_dim_location import create_dim_location
        from create_dim_staff import create_dim_staff
        from create_dim_design import create_dim_design
        from create_fact_sales import create_fact_sales
        from create_dim_currency import create_dim_currency
        from processing_utils import df_to_parquet, init_s3_client,  write_timestamp_to_s3, \
                read_timestamp_from_s3, write_parquet_file_to_s3,filter_files_by_timestamp, \
                list_objects_in_bucket
        from s3_file_reader import s3_reader_filtered
        
import botocore
import datetime
from botocore.exceptions import ClientError
import logging


logger = logging.getLogger('Processing Lambda Log')
logging.basicConfig()
logger.setLevel(logging.INFO)

def processed_lambda_handler(event={}, context={}):

        """
        This function takes timestamps from point X and Y to dictate which segment of data
        should be proessed and then converts the data into dataframes and restructures them 
        for the warehouse and stores this data in parquet format to be read into the warehouse
        by the warehouse lambda.

            Parameters:
                    event: Required for AWS lambda, however, this function takes no inputs.
                    context: Required for AWS lambda, however, this function takes no inputs.

            Returns:
                    Returns a message of success if the lambda runs without error and one of
                    failure if an error occurs.
        """
        
        try:

                s3_client=init_s3_client()
                ingestion_bucket="nc-team-reveries-ingestion"
                processed_bucket="nc-team-reveries-processing"



                end_timestep=read_timestamp_from_s3(ingestion_bucket, 'timestamp_start',s3_client)
                start_timestep=read_timestamp_from_s3(processed_bucket, 'timestamp',s3_client)    

                date_format = "%m:%d:%Y-%H:%M:%S"
                end_time= datetime.datetime.strptime(end_timestep, date_format)
                start_time= datetime.datetime.strptime(start_timestep, date_format)



                ingestion_table_names = [
                'design',
                'currency',
                'staff',
                'counterparty',
                'address',
                'department',
                'sales_order']

                df_dict={}
                for table in ingestion_table_names:
                        objects=list_objects_in_bucket(ingestion_bucket,table)
                        filtered_files=filter_files_by_timestamp(ingestion_bucket,table,objects, start_time, end_time)
                        df=s3_reader_filtered(table,filtered_files)
                        df_dict[table]=df
                
                star_schema_tables = dict()
                
                star_schema_tables['dim_date'] = create_dim_date()
                star_schema_tables['dim_counterparty'] = create_dim_counterparty(df_dict['address'], df_dict['counterparty'])
                star_schema_tables['dim_staff'] = create_dim_staff(df_dict['staff'],df_dict['department'])
                star_schema_tables['dim_location'] = create_dim_location(df_dict['address'])
                star_schema_tables['dim_design'] = create_dim_design(df_dict['design'])
                star_schema_tables['dim_currency'] = create_dim_currency(df_dict['currency'])
                star_schema_tables['fact_sales_order'] = create_fact_sales(df_dict['sales_order'])
                
                for table,df in star_schema_tables.items():
                        parquet_data= df_to_parquet(df)
                        write_parquet_file_to_s3(parquet_data, s3_client, processed_bucket, table, start_time, end_time)



                date_time = end_time.strftime("%m:%d:%Y-%H:%M:%S")
                write_timestamp_to_s3(s3_client, processed_bucket, date_time)
                response = logger.info("-SUCCESS- Data processed successfully")
                return response
        


        except Exception:
                response = logger.error("-ERROR- Data processing failed")
                return response
