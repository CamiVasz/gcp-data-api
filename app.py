from avro import schema, io, datafile
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from fastapi import FastAPI, HTTPException
from google.cloud import storage, secretmanager
import pandas as pd
from io import BytesIO
from sqlalchemy import Table, Column, MetaData
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER, insert
from load_historical_data import connect_with_connector
from db_models import *

app = FastAPI()

Employees = Table(
    "hired_employees",
    MetaData(),
    Column("id", INTEGER, primary_key=True),
    Column("name", VARCHAR(255)),
    Column("datetime", VARCHAR(255)),
    Column("department_id", INTEGER),
    Column("job_id", INTEGER),
)
Departments = Table(
    "departments",
    MetaData(),
    Column("id", INTEGER, primary_key=True),
    Column("department", VARCHAR(255)),
)
Jobs = Table(
    "jobs",
    MetaData(),
    Column("id", INTEGER, primary_key=True),
    Column("job", VARCHAR(255))
)
tables = {"hired_employees": Employees, 
            "departments": Departments, 
            "jobs": Jobs}

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
    conn = connect_with_connector().connect()
    if table_name == "hired_employees":
        statement = Employees.insert().values({
            "id": bindparam("id"),
            "name": bindparam("name"),
            "datetime": bindparam("datetime"),
            "department_id": bindparam("department_id"),
            "job_id": bindparam("job_id")
            }
        )
    elif table_name == "departments":
        statement = Departments.insert().values({
            "id": bindparam("id"),
            "department": bindparam("department")
            }
        )
    elif table_name == "jobs":
        statement = Jobs.insert().values({
            "id": bindparam("id"),
            "job": bindparam("job")
            }
        )
    else:
        raise ValueError(f"Table {table_name} does not exist")
    conn.execute(statement, batch_data)
    conn.commit()
    conn.close()


@app.post("/batch-transactions/")
async def create_batch_transactions(batch_transaction):
    """
    Create batch transactions and insert them into the specified table.

    Parameters:
    - batch_transaction (dict): A dictionary containing the batch transaction data.
      - "data" (list): A list of dictionaries representing individual transactions.
      - "table_name" (str): The name of the table to insert data into. 
         Options are 'hired_employees', 'departments', or 'jobs'.

    Returns:
    - dict: A dictionary with a single key "message" and a value indicating 
            the success of the operation.

    Raises:
    - HTTPException: If an invalid table name is provided.
    """
    data = batch_transaction["data"]
    table_name = batch_transaction["table_name"]
    if table_name == "hired_employees":
        data = [EmployeeTransaction(**t) for t in data]
    elif table_name == "departments":
        data = [DepartmentTransaction(**t) for t in data]
    elif table_name == "jobs":
        data = [JobTransaction(**t) for t in data]
    else:
        raise HTTPException(
            status_code=400,
            detail="Invalid table name. Please use 'hired_employees', 'departments', or 'jobs'",
        )

    # Validate and insert batch data into the database
    try:
        # Insert batch data into the database
        insert_batch_data(
            table_name, [dict(transaction) for transaction in data]
        )

        return {"message": f"Batch transactions for {table_name} inserted successfully"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


def query_database(table_name: str):
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
    # Serialize data into AVRO format
    writer = DataFileWriter(open(file_name, "wb"), DatumWriter(), avro_schema)
    for record in data:
        writer.append(record)
    writer.close()

def upload_to_gcs(file_name):
    # Upload data to Google Cloud Storage
    client_storage = storage.Client()
    bucket_name = "globant-data"
    bucket = client_storage.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)

@app.post("/backup/{table_name}/")
async def backup_table(table_name: str):
    try:
        # Query data from the database
        data = query_database(table_name)

        # Define AVRO schema
        avro_schema = schema.parse(open(f"schemas/{table_name}.avsc").read())

        columns = [x.name for x in avro_schema.fields]
        data = pd.DataFrame.from_records(data, columns=columns)
        data = data.to_dict(orient="records")

        # Serialize data to AVRO format
        file_name = f"backup/{table_name}_backup.avro"
        serialize_to_avro(data, avro_schema, file_name)

        # Upload AVRO data to Google Cloud Storage
        upload_to_gcs(file_name)

        return {
            "message": f"Backup for {table_name} completed successfully. File uploaded to GCS: {file_name}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/restore-avro-data/{table_name}/")
def restore_avro_data_endpoint(table_name: str):
    try:
        if table_name not in tables.keys():
            raise ValueError(f"Table {table_name} does not exist")
        # Fetch Avro data from GCS
        storage_client = storage.Client()
        bucket = storage_client.get_bucket("globant-data")
        file_name = f"backup/{table_name}_backup.avro"
        blob = bucket.blob(file_name)
        avro_data_bytes = BytesIO(blob.download_as_bytes())

        avro_schema = schema.parse(open(f"schemas/{table_name}.avsc").read())

        # Read avro data
        reader = DatumReader(avro_schema)
        avro_file_reader = DataFileReader(avro_data_bytes, reader)
        avro_data = [record for record in avro_file_reader]
        avro_file_reader.close()

        insert_batch_data(table_name, avro_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import requests, json
    api_url = "http://localhost:8000"

    # Backup and restore data
    response = requests.post(
        f"{api_url}/backup/departments/"
    )
    print(response.status_code)
    print(response.json())
    response = requests.post(
        f"{api_url}/restore_avro_data/departments/"
    )
    print(response.status_code)
    print(response.json())
    
    # Load batch transactions from JSON file
    with open("batch_insert/employees.json") as f:
        batch_transaction = json.load(f)

    response = requests.post(
        f"{api_url}/batch-transactions/", json=batch_transaction
    )
    print(response.status_code)
    print(response.json())
