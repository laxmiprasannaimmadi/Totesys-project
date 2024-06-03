from src.processing.create_dim_location import create_dim_location
import pytest
import pandas as pd
import json


class TestLocationDimensionTable:
    def test_create_dim_location_returns_dataframe(self):
        with open("data/table_json_data_fake_ingestion_data/fakedata.json") as f:
            ingestion_address = json.load(f)["address"]

            dim_location = pd.DataFrame(ingestion_address)
        assert isinstance(create_dim_location(dim_location), pd.DataFrame)

    def test_create_dim_location_df_address_id_column_renamed_to_location_id(self):
        with open("data/table_json_data_fake_ingestion_data/fakedata.json") as f:
            ingestion_address = json.load(f)["address"]

        dim_location = pd.DataFrame(ingestion_address)
        dataframe_columns = create_dim_location(dim_location).columns

        assert "address_id" not in dataframe_columns
        assert "location_id" in dataframe_columns

    def test_create_dim_location_for_dropped_columns(self):
        with open("data/table_json_data_fake_ingestion_data/fakedata.json") as f:
            ingestion_address = json.load(f)["address"]

        dim_location = pd.DataFrame(ingestion_address)
        dataframe_columns = create_dim_location(dim_location).columns

        assert "created_at" not in dataframe_columns
        assert "last_updated" not in dataframe_columns
