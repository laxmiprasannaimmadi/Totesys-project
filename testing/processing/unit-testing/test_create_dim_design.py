import pytest
import json
from src.processing.create_dim_design import *
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
def design_df(create_test_data_df):
    design_df = create_test_data_df["design"]
    return design_df


class TestDimDesign:
    def test_func_is_df(self, design_df):
        assert isinstance(create_dim_design(design_df), pd.DataFrame)

    def test_column_names(self, design_df):
        expected_column_names = [
            "design_id",
            "design_name",
            "file_location",
            "file_name",
        ]
        result = create_dim_design(design_df)
        for col in result:
            assert col in expected_column_names
        for col in expected_column_names:
            assert col in result

    def test_length(self, design_df):
        expected_length = 2
        result = create_dim_design(design_df)
        assert result.shape[0] == expected_length