import pytest
import json
from src.processing.create_dim_counterparty import create_dim_counterparty
import pandas as pd


@pytest.fixture()
def counterparty_test_data():
    with open("./data/table_json_data_fake_ingestion_data/fakedata.json", "r") as f:
        return json.load(f)


@pytest.fixture()
def create_test_data_df(counterparty_test_data):
    dfs = dict()
    for k, v in counterparty_test_data.items():
        dfs[k] = pd.DataFrame(v)
    return dfs


@pytest.fixture()
def create_address_df(create_test_data_df):
    address_df = create_test_data_df["address"]
    return address_df


@pytest.fixture()
def create_counterparty_df(create_test_data_df):
    counterparty_df = create_test_data_df["counterparty"]
    return counterparty_df


class TestCreateDimCounterparty:
    def test_dim_counterparty_function_returns_a_df(
        self, create_address_df, create_counterparty_df
    ):
        result = create_dim_counterparty(
            address_df=create_address_df, counterparty_df=create_counterparty_df
        )
        assert isinstance(result, pd.DataFrame)

    def test_dim_counterparty_func_has_star_schema_columns(
        self, create_address_df, create_counterparty_df
    ):
        expected = [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone_number",
        ]
        result = create_dim_counterparty(
            address_df=create_address_df, counterparty_df=create_counterparty_df
        )
        for i in expected:
            assert i in result.columns
        for i in result.columns:
            assert i in expected

    def test_input_df_unchanged(self, create_address_df, create_counterparty_df):
        copy_address_df = create_address_df.copy(deep=True)
        copy_counterparty_df = create_counterparty_df.copy(deep=True)
        result = create_dim_counterparty(
            address_df=create_address_df, counterparty_df=create_counterparty_df
        )
        assert create_address_df.equals(copy_address_df)
        assert create_counterparty_df.equals(copy_counterparty_df)

    def test_returns_expected_values(self):
        counterparty_data = {
            "counterparty_id": 1,
            "counterparty_legal_name": "Fahey and Sons",
            "legal_address_id": 1,
            "commercial_contact": "Micheal Toy",
            "delivery_contact": "Mrs. Lucy Runolfsdottir",
            "created_at": "2022-11-03T14:20:51.563000",
            "last_updated": "2022-11-03T14:20:51.563000",
        }
        input_counterparty = pd.DataFrame([counterparty_data])
        address_data = {
            "address_id": 1,
            "address_line_1": "6826 Herzog Via",
            "address_line_2": None,
            "district": "Avon",
            "city": "New Patienceburgh",
            "postal_code": "28441",
            "country": "Turkey",
            "phone": "1803 637401",
            "created_at": "2022-11-03T14:20:51.563000",
            "last_updated": "2022-11-03T14:20:51.563000",
        }
        input_address = pd.DataFrame([address_data])
        print(input_counterparty, input_address)
        expected = pd.DataFrame(
            [
                {
                    "counterparty_id": 1,
                    "counterparty_legal_name": "Fahey and Sons",
                    "counterparty_legal_address_line_1": "6826 Herzog Via",
                    "counterparty_legal_address_line_2": None,
                    "counterparty_legal_district": "Avon",
                    "counterparty_legal_city": "New Patienceburgh",
                    "counterparty_legal_postal_code": "28441",
                    "counterparty_legal_country": "Turkey",
                    "counterparty_legal_phone_number": "1803 637401",
                }
            ]
        )
        result = create_dim_counterparty(
            address_df=input_address, counterparty_df=input_counterparty
        )
        assert result.equals(expected)
