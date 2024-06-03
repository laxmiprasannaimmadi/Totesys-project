import json
import datetime
import botocore.session
from pg8000.native import identifier, literal
from decimal import Decimal


def init_s3_client():

    """
    Initialises an s3 client using boto3.

            Parameters:
                    No inputs are taken for this function.

            Returns:
                    An instance of s3 client.
    """
    try:
        session = botocore.session.get_session()
        s3_client = session.create_client("s3")
        return s3_client
    
    except Exception:
        raise ConnectionRefusedError("Failed to connect to s3 client")


def get_current_timestamp(s3_client):

    """
    Gets X timestamp to query data onward from.

            Parameters:
                    Requires a boto3 s3 client connection.

            Returns:
                    A timestamp in datetime format.
    """

    try:
        response = s3_client.get_object(
            Bucket="nc-team-reveries-ingestion", Key="timestamp"
        )

        body = response["Body"].read()
        dt_str = json.loads(body.decode("utf-8"))

        dt = datetime.datetime.fromisoformat(dt_str)
        return dt
    
    except Exception:
        raise TypeError(
            """An error occured accessing the timestamp from s3 bucket. 
                        Please check that there is a timestamp and it's format is correct"""
        )


def query_updated_table_information(conn, table, dt):

    """
    Querys the PSQL database for current information and returns it in a JSON format.

            Parameters:
                    conn: A working connection to a PSQL database.
                    table: The table name to draw data from.
                    dt: The timestamp from which you need to draw data onward from.

            Returns:
                    Queried data in a json format.
    """

    try:
        query = f"""SELECT *
                    FROM {identifier(table)}
                    WHERE last_updated > {literal(dt)}
                    ORDER BY last_updated ASC;"""
        result = conn.run(query)

        columns = [col["name"] for col in conn.columns]

        output_table = put_into_individual_table(table, result, columns)

        return output_table
    
    except Exception as e:
        print("An error occurred in DB query:", e)
        return None


def put_into_individual_table(table, result, columns):

    """
    Reformats the query data into JSON.

            Parameters:
                    table: The table name which is being formatted.
                    result: The query information.
                    columns: The 

            Returns:
                    JSON of data.
    """

    individual_table = {table: [dict(zip(columns, line)) for line in result]}

    return individual_table


def get_datestamp_from_table(individual_table, table_name):

    """
    Takes the 'last_updated' timestamp from the final entry in the data.

            Parameters:
                    individual_table: Table JSON data to find timestamp within.
                    table_name: Table's name to access the data within.

            Returns:
                    JSON of data.
    """

    try:
        timestamp = individual_table[table_name][-1]["last_updated"]
        return timestamp
    except Exception as e:
        raise e


def get_datetime_now():

    """
    Gets the realworld datetime to create a unique key for each segment of data.

            Parameters:
                    No inputs taken.

            Returns:
                    String format of current datetime in a month, day, year, hour, minute and second format.
    """

    now = datetime.datetime.now()
    date_time = now.strftime("%m:%d:%Y-%H:%M:%S")
    return date_time


def put_object_in_bucket(table_name, input_table, s3_client, bucket_name, dt_now):

    """
    Puts JSON data into s3 storage.

            Parameters:
                    table_name: Name of the table being stored.
                    input_table: The JSON data from the table being stored.
                    s3_client: Valid connection to s3 client.
                    bucket_name: The name of the bucket the data is being stored in.
                    dt_now: The current date to create a unique timestamp so data is not overwritten.

            Returns:
                    Response message from s3 client.
    """

    dt = s3_client.put_object(
        Body=json.dumps(input_table),
        Bucket=bucket_name,
        Key=f"{table_name}/--{dt_now}--{table_name}-data",
    )
    return dt


def put_timestamp_in_s3(timestamp, s3_client):

    """
    Puts timestamp data into s3 storage.

            Parameters:
                    timestamp: Updated timestamp to overwrite the existing one.
                    s3_client: Valid connection to s3 client.

            Returns:
                    Response message from s3 client.
    """
    
    try:
        dt = s3_client.put_object(
            Body=json.dumps(timestamp.isoformat()),
            Bucket="nc-team-reveries-ingestion",
            Key=f"timestamp",
        )
        return dt
    except Exception:
        raise RuntimeError("Check connection and formatting of timestamp")


def initialise_bucket_with_timestamp(s3_client):

    """
    Puts timestamp data into s3 storage.

            Parameters:
                    timestamp: Updated timestamp to overwrite the existing one.
                    s3_client: Valid connection to s3 client.

            Returns:
                    No output.
    """
     
    dt = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)
    response = s3_client.put_object(
        Body=json.dumps(dt.isoformat()),
        Bucket="nc-team-reveries-ingestion",
        Key=f"timestamp",
    )
    return response


def convert_datetimes_and_decimals(unconverted_json):

    """
    Converts datetimes and decimals to strings within the JSON file.

            Parameters:
                    unconverted_json: JSON to iterate over and apply changes.

            Returns:
                    JSON of data with isoformatted datetimes and decimals as strings.
    """
     
    for k, v in unconverted_json.items():
        for entry in v:
            for m, n in entry.items():
                if isinstance(n, datetime.datetime):
                    entry[m] = n.isoformat()
                elif isinstance(n, Decimal):
                    entry[m] = str(n)
    return unconverted_json


def add_ts_for_processing_bucket(s3_client, dt_now):

    """
    Updates the timestamp in the processing bucket to help select keys for processing in s3 bucket.

            Parameters:
                    s3_client: Valid boto3 s3 client.
                    dt_now: Current date time of ingestion.

            Returns:
                    Response from s3 client.
    """

    response = s3_client.put_object(
        Body=dt_now, Bucket="nc-team-reveries-ingestion", Key=f"timestamp_start"
    )
    return response


def initialise_process_bucket_with_timestamp(s3_client):

    """
    To be run with the creation of the lambda to begin the automatic process of ingesting data.
    Done by setting an initial timestamp related to the earliest given timestamp within the 
    original data set.

            Parameters:
                    s3_client: Valid boto3 s3 client.

            Returns:
                    Response from s3 client.
    """

    dt = datetime.datetime(2022, 1, 1, 1, 1, 1)
    date_time = dt.strftime("%m:%d:%Y-%H:%M:%S")
    response = s3_client.put_object(
        Body=date_time,
        Bucket="nc-team-reveries-processing",
        Key=f"timestamp",
    )
    return response
