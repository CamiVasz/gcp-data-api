from avro import schema, io, datafile
from avro.datafile import DataFileWriter
from avro.io import DatumReader, DatumWriter
from google.cloud import storage, secretmanager
from google.cloud.sql.connector import Connector, IPTypes
import subprocess, pg8000, sqlalchemy, os
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.dialects.postgresql import insert
from config import *


def retrieve_secret(secret_id, version_id="latest"):
    """
    Retrieves a secret from Google Secret Manager.

    Parameters:
    - secret_id (str): The ID of the secret to retrieve.
    - version_id (str, optional): The version of the secret to retrieve. 
        Defaults to "latest".

    Returns:
    - str: The secret payload data decodded as UTF-8.
    """
    client = secretmanager.SecretManagerServiceClient()
    secret_version_name = (
        f"projects/globant-api/secrets/{secret_id}/versions/{version_id}"
    )
    # Access the secret version
    response = client.access_secret_version(name=secret_version_name)
    # Return the secret payload
    return response.payload.data.decode("UTF-8")


def connect_with_connector() -> sqlalchemy.engine.base.Engine:
    """
    Initializes a connection pool for a Cloud SQL instance of Postgres.

    Uses the Cloud SQL Python Connector package to connect to the database,
    and the Secrets Manager to retrieve the database credentials.
    """

    instance_connection_name = retrieve_secret("instance_connection_name")
    db_user = retrieve_secret("db_user")
    db_pass = retrieve_secret("db_pass")
    db_name = retrieve_secret("db_name")

    ip_type = IPTypes.PRIVATE if os.environ.get("PRIVATE_IP") else IPTypes.PUBLIC

    # initialize Cloud SQL Python Connector object
    connector = Connector()

    def getconn() -> pg8000.dbapi.Connection:
        conn: pg8000.dbapi.Connection = connector.connect(
            instance_connection_name,
            "pg8000",
            user=db_user,
            password=db_pass,
            db=db_name,
            ip_type=ip_type,
        )
        return conn

    # The Cloud SQL Python Connector can be used with SQLAlchemy
    # using the 'creator' argument to 'create_engine'
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )
    return pool


def insert_batch_data(table_name, batch_data):
    """
    Inserts batch data into the specified table.

    Parameters:
    - table_name (str): The name of the table to insert data into.
      Options are 'hired_employees', 'departments', or 'jobs'.
    - batch_data (list): A list of dictionaries.
      Each dictionary should contain the necessary fields for the specified table.

    Returns:
    - None
    """
    if table_name not in tables.keys():
        raise ValueError(f"Table {table_name} does not exist")
    conn = connect_with_connector().connect()
    table = tables[table_name]
    columns = [x.name for x in table.columns]
    parameter_dict = {}
    for column in columns:
        parameter_dict[column] = bindparam(column)
    statement = insert(table).values(parameter_dict)
    statement = statement.on_conflict_do_nothing(index_elements=["id"])
    conn.execute(statement, batch_data)
    conn.commit()
    conn.close()


def query_table(table_name: str):
    """
    Query data from the specified table.

    Parameters:
    - table_name (str): The name of the table to query data from.

    Returns:
    - list: A list of tuples representing the queried data.

    Raises:
    - ValueError: If the specified table does not exist.

    """
    # Use SQLAlchemy to query data from the specified table
    if table_name not in tables.keys():
        raise ValueError(f"Table {table_name} does not exist")
    engine = connect_with_connector()
    with engine.connect() as connection:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=connection)
        result = connection.execute(table.select())
        return result.fetchall()


def serialize_to_avro(data, avro_schema, file_name):
    """
    Serialize data into AVRO format and write it to a file.

    Parameters:
    - data: The data to be serialized. It should be an iterable of records.
    - avro_schema: The AVRO schema to be used for serialization.
    - file_name: The name of the file to write the serialized data to.

    Returns:
    None

    Example Usage:
    serialize_to_avro(data, avro_schema, file_name)
    """
    writer = DataFileWriter(open(file_name, "wb"), DatumWriter(), avro_schema)
    for record in data:
        writer.append(record)
    writer.close()

def upload_to_gcs(file_name):
    """
    Uploads a file to Google Cloud Storage.

    Args:
        file_name (str): The name of the file to be uploaded.

    """
    client_storage = storage.Client()
    bucket_name = "globant-data"
    bucket = client_storage.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)


def execute_query(query):
    """
    Execute the specified query and return the result.

    Parameters:
    - query (sqlalchemy.text): The SQL query to be executed.

    Returns:
    - result (list): A list of tuples representing the result of the query.
    """
    # Execute the specified query
    engine = connect_with_connector()
    with engine.connect() as connection:
        result = connection.execute(query)
    return result.fetchall()
