from src.processing.create_fact_sales import create_fact_sales
import json
import pytest
import pandas as pd
from datetime import date, time, datetime
from decimal import Decimal

@pytest.fixture()
def test_data():
    with open("./data/table_json_data_fake_ingestion_data/fakedata.json", "r") as f:
        return json.load(f)


@pytest.fixture()
def create_test_data_df(test_data):
    dfs = dict()
    for k, v in test_data.items():
        dfs[k] = pd.DataFrame(v)
    return dfs


@pytest.fixture()
def make_test_sales_data(create_test_data_df):
    test_data = create_test_data_df["sales_order"]
    return test_data


class TestFactSales:
    def test_func_returns_df(self, make_test_sales_data):
        result = create_fact_sales(make_test_sales_data)
        assert isinstance(result, pd.DataFrame)

    def test_func_doesnt_mutate_input(self, make_test_sales_data):
        data_copy = make_test_sales_data.copy(deep=True)
        create_fact_sales(make_test_sales_data)
        assert make_test_sales_data.equals(data_copy)

    def test_no_lost_rows(self, make_test_sales_data):
        rows = make_test_sales_data.shape[0]
        result = create_fact_sales(make_test_sales_data)
        fact_table_rows = result.shape[0]
        assert rows == fact_table_rows

    def test_fact_table_has_correct_columns(self, make_test_sales_data):
        expected_columns = {
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "design_id",
            "agreed_payment_date",
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        }
        result = create_fact_sales(make_test_sales_data)
        assert set(result.columns) == expected_columns

    def test_function_produces_expected_results(self):
        input = pd.DataFrame(
            [
                {
                    "sales_order_id": 5,
                    "created_at": "2022-11-03T14:20:52.186000",
                    "last_updated": "2022-11-03T14:20:52.186000",
                    "design_id": 7,
                    "staff_id": 18,
                    "counterparty_id": 4,
                    "units_sold": 49659,
                    "unit_price": "2.41",
                    "currency_id": 3,
                    "agreed_delivery_date": "2022-11-05",
                    "agreed_payment_date": "2022-11-08",
                    "agreed_delivery_location_id": 25,
                }
            ]
        )
        expected = pd.DataFrame(
            [
                {
                    "sales_order_id": 5,
                    "created_date": date.fromisoformat("2022-11-03"),
                    "created_time": time.fromisoformat("14:20:52.186000"),
                    "last_updated_date": date.fromisoformat("2022-11-03"),
                    "last_updated_time": time.fromisoformat("14:20:52.186000"),
                    "sales_staff_id": 18,
                    "counterparty_id": 4,
                    "units_sold": 49659,
                    "unit_price": 2.41,
                    "currency_id": 3,
                    "design_id": 7,
                    "agreed_payment_date": date.fromisoformat("2022-11-08"),
                    "agreed_delivery_date": date.fromisoformat("2022-11-05"),
                    "agreed_delivery_location_id": 25,
                }
            ]
        )
        result = create_fact_sales(input)
        assert result.equals(expected)
