import pytest
from unittest.mock import patch, MagicMock
from src.ingestion.ingestion_lambda_handler import (
    ingestion_lambda_handler,
)
from src.ingestion.connection import connect_to_db, close_connection
from botocore.exceptions import ClientError
from datetime import datetime


@pytest.fixture
def mock_event():
    return {"dummy_event": "data"}


@pytest.fixture
def mock_context():
    return MagicMock()


@pytest.fixture
def mock_conn():
    return MagicMock()


@pytest.fixture
def mock_s3_client():
    return MagicMock()


@pytest.fixture
def mock_table_data():
    return {
        "sales_order": [],
        "design": [],
        "currency": [],
        "staff": [],
        "counterparty": [],
        "address": [],
        "department": [],
        "purchase_order": [],
        "payment_type": [],
        "payment": [],
        "transaction": [],
    }


@pytest.fixture
def mock_latest_timestamp():
    return datetime(2023, 1, 1)


@pytest.fixture
def mock_dt_now():
    return datetime(2024, 5, 23)


@patch("src.ingestion.ingestion_lambda_handler.connect_to_db")
@patch("src.ingestion.ingestion_lambda_handler.init_s3_client")
@patch("src.ingestion.ingestion_lambda_handler.get_current_timestamp")
@patch("src.ingestion.ingestion_lambda_handler.get_datetime_now")
@patch("src.ingestion.ingestion_lambda_handler.add_ts_for_processing_bucket")
@patch("src.ingestion.ingestion_lambda_handler.query_updated_table_information")
@patch("src.ingestion.ingestion_lambda_handler.convert_datetimes_and_decimals")
@patch("src.ingestion.ingestion_lambda_handler.put_object_in_bucket")
@patch("src.ingestion.ingestion_lambda_handler.get_datestamp_from_table")
@patch("src.ingestion.ingestion_lambda_handler.put_timestamp_in_s3")
@patch("src.ingestion.ingestion_lambda_handler.close_connection")
def test_ingestion_lambda_handler_as_a_whole(
    mock_close_connection,
    mock_put_timestamp_in_s3,
    mock_get_datestamp_from_table,
    mock_put_object_in_bucket,
    mock_convert_datetimes_and_decimals,
    mock_query_updated_table_information,
    mock_add_ts_for_processing_bucket,
    mock_get_datetime_now,
    mock_get_current_timestamp,
    mock_init_s3_client,
    mock_connect_to_db,
    mock_event,
    mock_context,
    mock_conn,
    mock_s3_client,
    mock_table_data,
    mock_latest_timestamp,
    mock_dt_now,
    ):

    mock_connect_to_db.return_value = mock_conn
    mock_init_s3_client.return_value = mock_s3_client
    mock_get_current_timestamp.return_value = mock_latest_timestamp
    mock_get_datetime_now.return_value = mock_dt_now
    mock_convert_datetimes_and_decimals.side_effect = lambda x: x
    mock_query_updated_table_information.return_value = mock_table_data

    ingestion_lambda_handler(mock_event, mock_context)

    # Assertions
    mock_connect_to_db.assert_called_once()
    mock_init_s3_client.assert_called_once()
    mock_get_current_timestamp.assert_called_with(mock_s3_client)
    mock_add_ts_for_processing_bucket.assert_called_with(mock_s3_client, mock_dt_now)
    mock_close_connection.assert_called_once_with(conn=mock_conn)
    mock_put_timestamp_in_s3.assert_called_once_with(
        mock_latest_timestamp, mock_s3_client
    )
    mock_get_current_timestamp.assert_called_with(mock_s3_client)
    for table in mock_table_data.keys():
        print("processing table:", table)
        mock_convert_datetimes_and_decimals.assert_any_call(mock_table_data)
        if len(mock_table_data[table]) > 0:
            mock_put_object_in_bucket.assert_any_call(
                table,
                mock_table_data,
                mock_s3_client,
                "nc-team-reveries-ingestion",
                mock_dt_now,
            )
            mock_get_datestamp_from_table.assert_any_call(mock_table_data, table)
        else:
            mock_put_object_in_bucket.assert_not_called()
            mock_get_datestamp_from_table.assert_not_called()