from src.processing.create_dim_staff import create_dim_staff
import pytest
import pandas as pd
import json


class TestStaffDimensionTable:
    def test_create_dim_staff_returns_dataframe(self):
        with open(
            "./data/table_json_data_fake_ingestion_data/fakedata.json", "r"
        ) as staff:
            ingestion_staff = json.load(staff)["staff"]
        with open(
            "./data/table_json_data_fake_ingestion_data/fakedata.json", "r"
        ) as dept:
            ingestion_dept = json.load(dept)["department"]

        dim_staff = pd.DataFrame(ingestion_staff)
        dim_dept = pd.DataFrame(ingestion_dept)
        assert isinstance(create_dim_staff(dim_staff, dim_dept), pd.DataFrame)

    def test_create_dim_staff_df_columns(self):
        with open(
            "./data/table_json_data_fake_ingestion_data/fakedata.json", "r"
        ) as staff:
            ingestion_staff = json.load(staff)["staff"]
        with open(
            "./data/table_json_data_fake_ingestion_data/fakedata.json", "r"
        ) as dept:
            ingestion_dept = json.load(dept)["department"]

        expected_columns = [
            "staff_id",
            "first_name",
            "last_name",
            "email_address",
            "department_name",
            "location",
        ]
        dim_staff = pd.DataFrame(ingestion_staff)
        dim_dept = pd.DataFrame(ingestion_dept)
        dataframe_columns = create_dim_staff(dim_staff, dim_dept).columns

        assert "created_at" not in dataframe_columns
        assert list(dataframe_columns) == expected_columns

    def test_create_dim_staff_for_matching_dept_id(self):
        with open(
            "./data/table_json_data_fake_ingestion_data/fakedata.json", "r"
        ) as staff:
            ingestion_staff = json.load(staff)["staff"]
        with open(
            "./data/table_json_data_fake_ingestion_data/fakedata.json", "r"
        ) as dept:
            ingestion_dept = json.load(dept)["department"]

        dim_staff = pd.DataFrame(ingestion_staff)
        dim_dept = pd.DataFrame(ingestion_dept)
        mod_dim_staff = create_dim_staff(dim_staff, dim_dept)
        assert len(mod_dim_staff) == 1
