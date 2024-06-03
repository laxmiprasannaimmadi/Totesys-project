import pytest
import json
from src.processing.create_dim_currency import create_dim_currency
import pandas as pd


@pytest.fixture()
def currency_test_data():
    with open("./data/table_json_data_fake_ingestion_data/fakedata.json", "r") as f:
        return json.load(f)


@pytest.fixture()
def create_test_data_df(currency_test_data):
    return pd.DataFrame(currency_test_data["currency"])


class TestCreateDimCurrency:
    def test_dim_currency_function_returns_a_df(self, create_test_data_df):
        result = create_dim_currency(create_test_data_df)
        assert isinstance(result, pd.DataFrame)

    def test_dim_currency_func_has_expected_columns(self, create_test_data_df):
        expected = ["currency_id", "currency_code", "currency_name"]
        result = create_dim_currency(create_test_data_df)
        for column in expected:
            assert column in result.columns
        for column in result.columns:
            assert column in expected

    def test_input_df_unchanged(self, create_test_data_df):
        copy_df = create_test_data_df.copy(deep=True)
        result = create_dim_currency(create_test_data_df)
        assert create_test_data_df.equals(copy_df)

    def test_returns_expected_values(self):
        with open(
            "./data/table_json_data_fake_ingestion_data/fakedata.json", "r"
        ) as file:
            currency_data = json.load(file)
        body = currency_data
        currency_list = body["currency"]
        input_df = pd.DataFrame(currency_list)
        expected_data = {
            "currency_id": [1, 2, 3],
            "currency_code": ["GBP", "USD", "EUR"],
            "currency_name": ["Pound Sterling", "US Dollar", "Euro"],
        }
        expected_df = pd.DataFrame(expected_data)
        result = create_dim_currency(input_df)
        assert result.equals(expected_df)
