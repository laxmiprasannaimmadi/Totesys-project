import pytest
from src.ingestion.ingestion_utils import (
    get_current_timestamp,
    get_datestamp_from_table,
    get_datetime_now,
    put_into_individual_table,
    put_object_in_bucket,
    query_updated_table_information,
    convert_datetimes_and_decimals,
    init_s3_client,
    put_timestamp_in_s3,
    initialise_bucket_with_timestamp,
    add_ts_for_processing_bucket,
    initialise_process_bucket_with_timestamp,
)
import datetime
from unittest.mock import Mock, patch, MagicMock
from botocore.exceptions import ClientError
from pg8000.native import literal, identifier
from pg8000.native import Connection
from dotenv import load_dotenv
from moto import mock_aws
import boto3
import os
import json
import datetime
from datetime import datetime as dt_mod
from decimal import Decimal


@pytest.fixture
def get_table_names():
    tables = [
        "counterparty",
        "currency",
        "department",
        "design,staff",
        "sales_order",
        "address",
        "payment",
        "purchase_order",
        "payment_type",
        "transaction",
    ]
    return tables


@pytest.fixture(scope="class")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def mock_s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3")


class TestInitS3Client:

    @pytest.mark.it("Test client connection success")
    @patch("botocore.session.get_session")
    def test_client_connection(self, mocked_session):
        mock_session_rv = mocked_session.return_value
        mock_client = mock_session_rv.create_client.return_value
        s3_client = init_s3_client()

        assert mock_session_rv.create_client.called_once_with("s3")
        assert s3_client == mock_client

    @pytest.mark.it("Test client connection failure")
    @patch("botocore.session.get_session")
    def test_client_connection_failure(self, mocked_session):
        mock_session_rv = mocked_session.return_value
        mock_session_rv.create_client.side_effect = Exception

        with pytest.raises(ConnectionRefusedError):
            init_s3_client()


class TestGetCurrentTimestamp:
    def test_get_currrent_timestamp_returns_a_datetime_object(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        mock_s3_client.put_object(
            Bucket="nc-team-reveries-ingestion",
            Key="timestamp",
            Body=json.dumps("2024-05-16T12:00:00"),
        )

        result = get_current_timestamp(mock_s3_client)
        assert result.isoformat() == "2024-05-16T12:00:00"

    def test_raises_error_with_no_current_timestamp(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        with pytest.raises(Exception):
            get_current_timestamp(mock_s3_client)

    def test_when_timestamp_format_is_incorrect(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_s3_client.put_object(
            Bucket="nc-team-reveries-ingestion",
            Key="timestamp",
            Body="incorrect test string",
        )
        with pytest.raises(Exception):
            get_current_timestamp(mock_s3_client)


@pytest.fixture
def mock_conn():
    return MagicMock()


@pytest.fixture
def mock_conn1():
    class MockConnection:
        def __init__(self):
            self.columns = [{"name": "id"}, {"name": "name"}, {"name": "last_updated"}]

        def run(self, query):
            raise Exception("Database error")

    return MockConnection()


class TestQueryUpdatedTableInformation:

    def test_query_updated_table_information_returns_correct_data_format(
        self, mock_conn
    ):

        mock_conn.run.return_value = [
            {"id": 1, "name": "Cameron", "last_updated": "2024-05-16T12:00:00"},
            {"id": 2, "name": "Luke", "last_updated": "2024-05-16T13:00:00"},
            {"id": 3, "name": "Cameroon", "last_updated": "2024-05-16T16:00:00"},
            {"id": 4, "name": "Lucke", "last_updated": "2024-05-16T13:00:00"},
        ]
        mock_conn.columns = [{"name": "id"}, {"name": "name"}, {"name": "last_updated"}]
        dt = "2024-05-16T11:00:00"
        table = "test_table"
        result = query_updated_table_information(mock_conn, table, dt)
        assert isinstance(result["test_table"], list)
        assert isinstance(result["test_table"][0], dict)

    def test_query_updated_table_information_handles_database_error(mock_conn):
        output_table = query_updated_table_information(
            mock_conn1, "test_table", "2024-05-16T11:00:00"
        )
        assert output_table is None


class TestPutIntoIndividualTable:
    def test_put_into_individual_table_returns_dict(self):
        table = "test_sales_order"
        columns = [
            "sales_order_id",
            "created_at",
            "last_updated",
            "design_id",
            "staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
        ]
        result = [
            [
                8256,
                "2024-05-14 16:54:10.308",
                "2024-05-14 16:54:10.308",
                311,
                1,
                20,
                99847,
                3.80,
                2,
                "2024-05-18",
                "2024-05-18",
                6,
            ]
        ]
        individual_table = put_into_individual_table(table, result, columns)
        assert isinstance(individual_table, dict)

    def test_into_individual_table_matches_correct_key_pairs(self):
        table = "test_sales_order"
        columns = [
            "sales_order_id",
            "created_at",
            "last_updated",
            "design_id",
            "staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "agreed_delivery_date",
            "agreed_payment_date",
            "agreed_delivery_location_id",
        ]
        result = [
            [
                8256,
                "2024-05-14 16:54:10.308",
                "2024-05-14 16:54:10.308",
                311,
                1,
                20,
                99847,
                3.80,
                2,
                "2024-05-18",
                "2024-05-18",
                6,
            ]
        ]
        individual_table = put_into_individual_table(table, result, columns)
        assert individual_table == {
            table: [
                {
                    "sales_order_id": 8256,
                    "created_at": "2024-05-14 16:54:10.308",
                    "last_updated": "2024-05-14 16:54:10.308",
                    "design_id": 311,
                    "staff_id": 1,
                    "counterparty_id": 20,
                    "units_sold": 99847,
                    "unit_price": 3.80,
                    "currency_id": 2,
                    "agreed_delivery_date": "2024-05-18",
                    "agreed_payment_date": "2024-05-18",
                    "agreed_delivery_location_id": 6,
                }
            ]
        }


class TestGetDatestampFromTable:

    def test_get_datestamp_from_table_returns_timestamp(self):
        # timestamp should = datetime.datetime(2022, 1, 1, 1, 1, 1, 111111)
        test_table_name = "sales_order"
        fake_data = {
            test_table_name: [
                {"last_updated": "2022-03-12 19:24:01:377"},
                {"last_updated": "2024-05-14 16:45:09.72"},
                {"last_updated": "2024-05-14 16:54:10.308"},
            ]
        }
        result = get_datestamp_from_table(fake_data, test_table_name)
        expected = "2024-05-14 16:54:10.308"
        assert result == expected
        assert type(result) == str
        assert len(result) == len(expected)

    def test_get_datestamp_from_table_error_handling(self):
        test_table = {"test": []}
        with pytest.raises(IndexError):
            get_datestamp_from_table(test_table, "test")

    def test_get_datetime_now_converts_to_strftime(self):
        test_func = get_datetime_now()
        now = datetime.datetime.now()
        expected = now.strftime("%m:%d:%Y-%H:%M:%S")
        assert test_func == expected
        assert type(test_func) == str


class TestGetDatetimeNow:
    pytest.mark.it("Tests get datetime now")

    def test_gdtn(self):
        dt = get_datetime_now()
        formatted_dt = dt_mod.strptime(dt, "%m:%d:%Y-%H:%M:%S")

        assert isinstance(formatted_dt, datetime.datetime)


class TestPutObjectInBucket:
    def test_func_puts_obj_in_s3(self, mock_s3_client):
        table_name = "test_table"
        individual_table = {
            table_name: [
                {"tom:": "cat"},
                {"tom:": "cat"},
                {"date": datetime.datetime(2022, 1, 1, 1, 1, 1, 1).isoformat()},
                {"num": str(Decimal(3.14))},
            ]
        }
        bucket = "testbucket"
        mock_s3_client.create_bucket(
            Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
        )
        dt_now = datetime.datetime.now()
        put_object_in_bucket(
            table_name, individual_table, mock_s3_client, bucket, dt_now
        )
        listed_objects = mock_s3_client.list_objects(Bucket=bucket)
        returned_object = mock_s3_client.get_object(
            Bucket=bucket, Key=listed_objects["Contents"][0]["Key"]
        )
        body = returned_object["Body"].read()
        result = json.loads(body.decode("utf-8"))
        assert result == individual_table


class TestPutTimestampInS3:
    pytest.mark.it("Tests for success response from mock AWS")

    def test_put_object_into_bucket(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        timestamp = datetime.datetime(2022, 1, 1, 1, 1, 1, 1)
        action = put_timestamp_in_s3(timestamp, mock_s3_client)

        assert action["ResponseMetadata"]["HTTPStatusCode"] == 200

    pytest.mark.it(
        "Tests raise connection error for failure of timestamp input due to no isoformat attr"
    )

    def test_put_object_into_bucket(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        timestamp = "good morning"
        with pytest.raises(RuntimeError):
            print(put_timestamp_in_s3(timestamp, mock_s3_client))


class TestInitBucketWithTimestamp:
    pytest.mark.it("Tests status code 200 when running function")

    def test_put_timestamp_into_bucket(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        action = initialise_bucket_with_timestamp(mock_s3_client)

        assert action["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestConvertDatetimesAndDecimals:
    pytest.mark.it("Converts data formats of datetime and decimal to str")

    def test_converts_dt_and_deci_format(self):
        data = {
            "transaction": [
                {
                    "transaction_id": 1,
                    "transaction_type": "PURCHASE",
                    "sales_order_id": None,
                    "purchase_order_id": 2,
                    "created_at": datetime.datetime(2022, 11, 3, 14, 20, 52, 186000),
                    "last_updated": datetime.datetime(2022, 11, 3, 14, 20, 52, 186000),
                },
                {
                    "transaction_id": 3,
                    "transaction_type": "SALE",
                    "sales_order_id": 1,
                    "purchase_order_id": None,
                    "created_at": datetime.datetime(2022, 11, 3, 14, 20, 52, 186000),
                    "last_updated": datetime.datetime(2022, 11, 3, 14, 20, 52, 186000),
                },
            ]
        }
        result = convert_datetimes_and_decimals(data)
        expected = {
            "transaction": [
                {
                    "transaction_id": 1,
                    "transaction_type": "PURCHASE",
                    "sales_order_id": None,
                    "purchase_order_id": 2,
                    "created_at": "2022-11-03T14:20:52.186000",
                    "last_updated": "2022-11-03T14:20:52.186000",
                },
                {
                    "transaction_id": 3,
                    "transaction_type": "SALE",
                    "sales_order_id": 1,
                    "purchase_order_id": None,
                    "created_at": "2022-11-03T14:20:52.186000",
                    "last_updated": "2022-11-03T14:20:52.186000",
                },
            ]
        }
        assert result == expected


class TestAddTSToProcessing:
    pytest.mark.it(
        "Checks status code of 200 when setting timestamp in processing bucket"
    )

    def test_put_timestamp_into_proc_bucket(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-ingestion",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        dt = datetime.datetime(2024, 1, 1, 1, 1, 1, 1)
        action = add_ts_for_processing_bucket(mock_s3_client, dt_now=dt.isoformat())

        assert action["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestInitProcessingBucketWithTimeStamp:
    pytest.mark.it(
        "Checks status code of 200 when setting init timestamp in processing bucket"
    )

    def test_put_init_timestamp_into_proc_bucket(self, mock_s3_client):
        mock_s3_client.create_bucket(
            Bucket="nc-team-reveries-processing",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        action = initialise_process_bucket_with_timestamp(mock_s3_client)

        assert action["ResponseMetadata"]["HTTPStatusCode"] == 200
