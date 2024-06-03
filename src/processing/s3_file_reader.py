import boto3
import json
import datetime
import pandas as pd
import awswrangler as wr
try:
    from src.processing.processing_utils import df_normalisation
except ModuleNotFoundError:
    from processing_utils import df_normalisation

def s3_reader_many_files(table):

    """
        Uses aws wrangler to read all files within an s3 bucket and key
        and return them as a dataframe.

            Parameters:
                    table: table name to read data from an s3 bucket.

            Returns:
                    Returns a dataframe.
    """

    bucket_name = 'nc-team-reveries-ingestion'
    file_key = table
    df = wr.s3.read_json(path=f's3://{bucket_name}/{file_key}/')
    if table in df.columns:
        df_norm = df_normalisation(df,table)
        return df_norm
    else:
        return df


def s3_reader_filtered(table,filtered_files):

    """
        Uses aws wrangler to read all files within an s3 bucket and key
        and return them as a dataframe.

            Parameters:
                    table: table name to read data from an s3 bucket.
                    filter_files: to and from points gathered using filter_files_by_timestamp function.

            Returns:
                    Returns a dataframe of given selection of files.
    """

    df = wr.s3.read_json(path=filtered_files)
    if table in df.columns:
        df_norm = df_normalisation(df,table)
        return df_norm
    else:
        return df

