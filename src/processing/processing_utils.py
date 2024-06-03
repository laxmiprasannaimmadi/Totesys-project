import pandas as pd
import json
import datetime
import awswrangler as wr
import botocore
from botocore.exceptions import ClientError

def df_normalisation(df,table_name):

    """
        Normalize semi-structured JSON data into a flat table.

            Parameters:
                    df[table_name]: the dataframe and it's table name to allow proper normalization
                    of the data.

            Returns:
                    Normalized dataframe of given data.
    """
    
    df_norm = pd.json_normalize(df[table_name])
    return df_norm

# purpose to read timestamp from ingestion bucket
def read_timestamp_from_s3(bucket, key,s3_client):

    """
        Normalize semi-structured JSON data into a flat table.

            Parameters:
                    df[table_name]: the dataframe and it's table name to allow proper normalization
                    of the data.

            Returns:
                    Normalized dataframe of given data.
    """

    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')
    return data

def extract_timestamp_from_key(key):

    """
        Seperates timestamp from key using -- pattern.

            Parameters:
                    key: key to extract timestamp from.

            Returns:
                    Datetime object in Month, Day, Year, Hour, Minute, Second format.
    """

    timestamp_str = key.split('--')[1]
    return datetime.datetime.strptime(timestamp_str, '%m:%d:%Y-%H:%M:%S')


def filter_files_by_timestamp(bucket_name, prefix, objects, start_time, end_time):

    """
        Creates a filtered list of object names gathered from the s3 bucket using timestamps to determine which objects are needed. 

            Parameters:
                    bucket_name: the s3 bucket name to gather data from.
                    prefix: the table name in this case or a general s3 key.
                    objects: a list of s3 object names to filter.
                    start_time: the time objects should be gathered from.
                    end_time: the time objects should be gathered up to.

            Returns:
                    List of filtered file names.
    """

    filtered_files = []
    for obj in objects:
        key = obj.split('/')[-1]
        timestamp = extract_timestamp_from_key(key)
        if timestamp and start_time <= timestamp <= end_time:
            filtered_files.append(f's3://{bucket_name}/{prefix}/{key}')
    return filtered_files

def df_to_parquet(df):

    """
        Converts df to parquet.

            Parameters:
                    df: dataframe to convert to parquet.

            Returns:
                    Dataframe in parquet format.
    """

    return df.to_parquet()


def list_objects_in_bucket(bucket_name,prefix):

    """
        Lists objects within a given bucket and key.

            Parameters:
                    bucket_name: name of bucket to list objects within.
                    prefix: s3 key to look within.

            Returns:
                    Dataframe in parquet format.
    """

    objects = wr.s3.list_objects(f's3://{bucket_name}/{prefix}/')
    return objects

def write_parquet_file_to_s3(file, s3_client, bucket_name, table_name, date_start, date_end):

    """
        Writes given parquet file to s3 bucket.

            Parameters:
                    file: file to be written to s3 bucket.
                    s3_client: valid s3 client.
                    bucket_name: s3 bucket to be written to.
                    table_name: table's name to be used as a key
                    date_start: date start to be used as a key
                    date_end: date end to be used as a key

            Returns:
                    Response from s3 client.
    """

    key = f"{table_name}/{date_start}_{date_end}_entries"
    response = s3_client.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=file 
    )
    return response

def init_s3_client():

    """
        Creates an s3 client with boto3.

            Parameters:
                    No inputs required.

            Returns:
                    s3 Client.
    """

    try: 
        session = botocore.session.get_session()
        s3_client = session.create_client("s3")
        return s3_client
    except ClientError as e:
        raise e("-ERROR- Failure to connect to s3 client, please check credentials")
                


def write_timestamp_to_s3(s3_client, bucket_name, timestamp):

    """
        Writes current end timestamp to s3 to dictate the next starting processing point.

            Parameters:
                    s3_client: valid s3 client.
                    bucket_name: name of bucket to write to.
                    timestamp: new timestamp to write into bucket.

            Returns:
                    Response from s3 client.
    """

    response = s3_client.put_object(
        Body= timestamp,
        Bucket=bucket_name,
        Key='timestamp'
    )
    return response

def initialise_processing_bucket_with_timestamp(s3_client):

    """
        Creates the initial timestamp for processing bucket to check files from.

            Parameters:
                    s3_client: valid s3 client.

            Returns:
                    Response from s3 client.
    """

    dt = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)
    response = s3_client.put_object(
        Body=json.dumps(dt.isoformat()),
        Bucket="nc-team-reveries-processing",
        Key=f"timestamp",
    )
    return response
