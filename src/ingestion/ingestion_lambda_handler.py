try:
    from src.ingestion.connection import connect_to_db, close_connection
except ModuleNotFoundError:
    from connection import connect_to_db, close_connection
from datetime import datetime

try:
    from src.ingestion.ingestion_utils import (
        init_s3_client,
        put_object_in_bucket,
        query_updated_table_information,
        get_datestamp_from_table,
        get_current_timestamp,
        put_timestamp_in_s3,
        convert_datetimes_and_decimals,
        add_ts_for_processing_bucket,
        get_datetime_now,
    )
except ModuleNotFoundError:
    from ingestion_utils import (
        init_s3_client,
        put_object_in_bucket,
        query_updated_table_information,
        get_datestamp_from_table,
        get_current_timestamp,
        put_timestamp_in_s3,
        convert_datetimes_and_decimals,
        add_ts_for_processing_bucket,
        get_datetime_now,
    )
import logging
from botocore.exceptions import ClientError


def ingestion_lambda_handler(event, context):
    """
    Queries PSQL database to gather all data rows from X timestamp up to Y timestamp
    and stores them in an s3 Bucket and updates the X timestamp with Y timestamp to
    query only new data.

            Parameters:
                    event: Required for AWS lambda, however, this function takes no inputs.
                    context: Required for AWS lambda, however, this function takes no inputs.

            Returns:
                    No output returned, however, cloudwatch logs a completion message if successfully run
                    or a failure message if an error occurs.
    """

    logger = logging.getLogger("Ingestion Lambda Log")
    logging.basicConfig()
    logger.setLevel(logging.INFO)

    logger.info("Ingestion process beginning")

    try:
        conn = connect_to_db()
    except ClientError as e:
        logger.error("-ERROR- No connection to DB returned")
        raise e("No connection to DB returned")

    try:
        s3_client = init_s3_client()
    except Exception as e:
        logger.error("-ERROR- S3 client failed")
        raise e("S3 client failed")

    try:
        dt = get_current_timestamp(s3_client)
        latest_timestamp = get_current_timestamp(s3_client)
    except Exception as e:
        logger.error(
            """-ERROR- An error occured accessing the timestamp from s3 bucket. 
                        Please check that there is a timestamp and it's format is correct"""
        )
        raise e(
            "An error occured accessing the timestamp from s3 bucket. Please check that there is a timestamp and it's format is correct"
        )

    dt_now = get_datetime_now()
    add_ts_for_processing_bucket(s3_client, dt_now)

    table_names = [
        "sales_order",
        "design",
        "currency",
        "staff",
        "counterparty",
        "address",
        "department",
        "purchase_order",
        "payment_type",
        "payment",
        "transaction",
    ]

    for table in table_names:
        try:
            individual_table = convert_datetimes_and_decimals(
                query_updated_table_information(conn, table, dt)
            )
        except Exception as e:
            logger.error("-ERROR- An error occurred in DB query for %s table", table)
            raise e("An error occurred in DB queryfor %s table", table)

        if len(individual_table[table]) > 0:
            try:
                put_object_in_bucket(
                    table,
                    individual_table,
                    s3_client,
                    "nc-team-reveries-ingestion",
                    dt_now,
                )
            except Exception as e:
                logger.error(
                    "-ERROR- Failed to put object in bucket at %s table", table
                )
                raise e("Failed to put object in bucketat %s table", table)

        if len(individual_table[table]) > 0:
            try:
                potential_timestamp = get_datestamp_from_table(individual_table, table)
            except Exception as e:
                logger.error(
                    "-ERROR- couldn't retrieve datestamp from table at %s table", table
                )
                raise e("couldn't retrieve datestamp from tableat %s table", table)

            dt_potential_timestamp = datetime.fromisoformat(potential_timestamp)

            if dt_potential_timestamp > latest_timestamp:
                latest_timestamp = dt_potential_timestamp

    try:
        put_timestamp_in_s3(latest_timestamp, s3_client)
    except Exception as e:
        logger.error(
            "-ERROR- failed to put timestamp in bucket with timestamp %s",
            latest_timestamp,
        )
        raise e("failed to put timestamp in bucketwith timestamp %s", latest_timestamp)

    logger.info("-STARTPROCESSING- Ingestion Process is complete.")

    close_connection(conn=conn)
