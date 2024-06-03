from src.ingestion.connection import connect_to_db, close_connection
from pg8000.native import Connection
import pytest
from unittest.mock import patch, MagicMock


class TestConnectToDB:
    @pytest.mark.it("Check return of connection type")
    def test_new_connect_to_db_psql_connection(self):
        assert isinstance(connect_to_db(), Connection)


class TestCloseConnection:
    @pytest.mark.it("Successfully closes db connection")
    def test_close_db_connection(self):
        mock_conn = MagicMock()
        close_connection(mock_conn)
        assert mock_conn.close.called_once()

    @pytest.mark.it("Error raised when failing to close connection")
    def test_close_connection_error(self):

        mock_connection = MagicMock()
        mock_connection.close.side_effect = ConnectionError()

        with pytest.raises(ConnectionError):
            close_connection(mock_connection)
