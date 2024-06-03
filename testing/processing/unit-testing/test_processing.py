from src.processing.s3_file_reader import (
    s3_reader_many_files,
    s3_reader_filtered,
)
from src.processing.processing_utils import df_normalisation
import pytest
import json
import boto3
import os
import pandas as pd
from moto import mock_aws
from unittest.mock import patch

with open("data/table_json_data_fake_ingestion_data/fakedata.json") as f:
    test_transaction_data = json.load(f)


@pytest.fixture(scope="function")
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_creds):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture
def dummy_ingestion_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="dummy_ingestion_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    with open("data/table_json_data_fake_ingestion_data/fakedata.json") as f:
        transaction_data = json.load(f)

    s3_client.put_object(
        Body=json.dumps(transaction_data),
        Bucket="dummy_ingestion_bucket",
        Key="test_data.json",
    )

@pytest.fixture
def test_data_to_df():
    with open("data/table_json_data_fake_ingestion_data/fakedata.json") as f:
        test_data = json.load(f)
    df = pd.DataFrame(test_data["transaction"])
    return df


class TestS3ReaderManyFiles:
    def test_s3_reader_many_files_returns_correct_data(self, mocker):
        df_result = pd.read_pickle("testing/processing/transaction_df.pkl")
        mocker.patch("awswrangler.s3.read_json", return_value=df_result)
        table_name = "transaction"
        result_df = s3_reader_many_files(table_name)
        assert isinstance(result_df, pd.DataFrame)
        assert result_df.shape == (10, 6)
        assert list(result_df.columns) == [
            "transaction_id",
            "transaction_type",
            "sales_order_id",
            "purchase_order_id",
            "created_at",
            "last_updated",
        ]
        assert result_df["created_at"][0] == "2022-11-03T14:20:52.186000"

    def test_s3_reader_filtered_returns_filtered_data(self, mocker, test_data_to_df):
        df_result = test_data_to_df
        mocker.patch("awswrangler.s3.read_json", return_value=df_result.head())
        table = "sales_order"
        result_df = s3_reader_filtered(table, [])
        assert isinstance(result_df, pd.DataFrame)
        assert result_df.shape == (5, 6)
        assert list(result_df.columns) == [
            "transaction_id",
            "transaction_type",
            "sales_order_id",
            "purchase_order_id",
            "created_at",
            "last_updated",
        ]
