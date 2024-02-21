"""
This script is designed to load csv data (from a local source)
into a GCP PostgresSQL database.
"""

from google.cloud import storage, secretmanager
from google.cloud.sql.connector import Connector, IPTypes
import subprocess, pg8000, sqlalchemy, os


def retrieve_secret(secret_id, version_id="latest"):
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
        # ...
    )
    return pool


def create_tables():
    """
    Creates tables in the database.

    - hired_employees
    - departments
    - jobs

    Parameters:
        None

    Returns:
        None
    """
    pool = connect_with_connector()
    with pool.connect() as db_conn:
        # Create hired employees table
        db_conn.execute(
            sqlalchemy.text(
                "CREATE TABLE IF NOT EXISTS hired_employees"
                "( id INTEGER, name VARCHAR(255), "
                "datetime VARCHAR(255) NOT NULL, department_id INTEGER NOT NULL, "
                "job_id INTEGER NOT NULL,"
                "PRIMARY KEY (id));"
            )
        )
        # Create departments table
        db_conn.execute(
            sqlalchemy.text(
                "CREATE TABLE IF NOT EXISTS departments"
                "( id INTEGER, department VARCHAR(255), "
                "PRIMARY KEY (id));"
            )
        )
        # Create jobs table
        db_conn.execute(
            sqlalchemy.text(
                "CREATE TABLE IF NOT EXISTS jobs"
                "( id INTEGER, job VARCHAR(255), "
                "PRIMARY KEY (id));"
            )
        )
        db_conn.commit()
        results = db_conn.execute(sqlalchemy.text("SELECT * FROM departments")).fetchall()


def cloud_sql_import(filename):
    """
    Imports the data from a CSV file on Google Storage
    into a Google Cloud SQL database.

    Parameters:
        filename (str): The name of the CSV file to be imported.

    Returns:
        None
    """
    gcloud_command = [
    'gcloud', 'sql', 'import', 'csv',
    'globant-pg',
    'gs://globant-data/' + filename,
    '--database=postgres',
    f'--table={filename.split(".")[0]}',
    '--user=admin',  # Replace with your PostgreSQL username
]

    # Execute the gcloud command
    subprocess.run(gcloud_command, check=True, input="Y\n", text=True)

"""
Uploads a file to Google Cloud Storage.

Parameters:
    filename (str): The name of the file to be uploaded.

Returns:
    None
"""
def load_data_to_storage(filename):
    client_storage = storage.Client()
    bucket_name = "globant-data"
    bucket = client_storage.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_filename(f'data/{filename}')

"""
This function is the entry point of the script and performs the following tasks:
1. Creates tables in the database using the 'create_tables' function.
2. Retrieves a list of files from the 'data' directory.
3. Loads each file to the Google Cloud Storage using the 'load_data_to_storage' function.
4. Imports the data from each file into the Google Cloud SQL database using the 'cloud_sql_import' function.

Parameters:
    None

Returns:
    None
"""
def main():
    # Create tables in database
    create_tables()
    files = os.listdir('data')
    # Load data to storage and then to database
    for file in files:
        load_data_to_storage(file)
        cloud_sql_import(file)

if __name__ == '__main__':
    main()
