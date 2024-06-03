from src.processing.create_dim_date import create_dim_date
import pytest
import pandas as pd


class TestDateDimensionTable:
    def test_create_dim_date_returns_a_dataframe(self):
        result = create_dim_date()
        assert isinstance(result, pd.DataFrame)

    def test_create_dim_date_has_correct_columns(self):
        expected_columns = [
            "date_id",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "month_name",
            "quarter",
        ]
        result = list(create_dim_date().columns)
        assert result == expected_columns

    def test_create_dim_date_has_correct_data_types(self):
        expected_data_types = {
            "year": "int32",
            "month": "int32",
            "day": "int32",
            "day_of_week": "int32",
            "day_name": "object",
            "month_name": "object",
            "quarter": "int32",
        }
        df = create_dim_date()
        for col, type in expected_data_types.items():
            assert df[col].dtype == type

    def test_create_dim_date_returns_correct_data_for_index(self):
        result = create_dim_date()
        assert result["day_of_week"][0] == 5
        assert result["year"][4] == 2022
        assert result["day_name"][868] == "Saturday"
        assert result["month_name"][867] == "May"

    # def test_create_dim_date_returns_correct_row_data(self):
    #     data = {'date_id': [pd.Timestamp('2022-01-01')],
    #         'year': [2022],
    #         'month': [1],
    #         'day': [1],
    #         'day_of_week': [5],
    #         'day_name': ['Saturday'],
    #         'month_name': ['January'],
    #         'quarter': [pd.Period('2022Q1', freq='Q')]
    #         }
    #     expected_row = pd.DataFrame(data)

    #     print(expected_row)

    #     result=create_dim_date()
    #     assert result.iloc[0] == expected_row
