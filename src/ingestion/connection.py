from botocore.exceptions import ClientError
from botocore.session import get_session
import json
from pg8000.native import Connection


def connect_to_db():
    """
    Gathers secrets from AWS to fill in the information required
    to create a connection to the PSQL database using pg8000.

            Parameters:
                    No inputs are taken for this function.

            Returns:
                    The database connection.
    """

    secret_name = "team_reveries_PSQL"
    region_name = "eu-west-2"
    session = get_session()
    client = session.create_client("secretsmanager", region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response["SecretString"]
        secret_value = json.loads(secret)
        username = secret_value["username"]
        password = secret_value["password"]
        database = secret_value["dbname"]
        host = secret_value["host"]
        port = secret_value["port"]
        return Connection(
            username,
            password=password,
            database=database,
            host=host,
            port=port,
        )
    except ClientError as e:
        raise e("No connection to DB returned")


def close_connection(conn):
    """
    Closes connection to the PSQL database

            Parameters:
                    conn: connection to pg8000 database.

            Returns:
                    No output.
    """

    try:
        conn.close()
    except ConnectionError:
        raise ConnectionError("Failed to close DB connection")
