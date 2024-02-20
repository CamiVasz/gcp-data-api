from avro import schema, io, datafile
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from fastapi import FastAPI, HTTPException
from google.cloud import storage, secretmanager
import pandas as pd
from io import BytesIO
from sqlalchemy.sql.expression import bindparam
from sqlalchemy.dialects.postgresql import insert
import sqlalchemy
from load_historical_data import connect_with_connector
from config import *
from utils import *

app = FastAPI()

@app.post("/batch-transactions/")
async def create_batch_transactions(batch_transaction: dict):
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
    if table_name not in transactions.keys():
        raise HTTPException(
            status_code=400,
            detail="Invalid table name. Please use 'hired_employees', 'departments', or 'jobs'",
        )
    insert_data = []
    not_conforming_transactions = []
    for transaction in data:
        try:
            validate = transactions[table_name](**transaction)
            insert_data.append(transaction)
        except:
            not_conforming_transactions.append(transaction)
            continue

    # Validate and insert batch data into the database
    try:
        # Insert batch data into the database
        insert_batch_data(table_name, insert_data)

        return {
            "message": f"""Batch transactions for {table_name} inserted successfully"""
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/backup/{table_name}/")
async def backup_table(table_name: str):
    try:
        # Query data from the database
        data = query_table(table_name)

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
async def restore_avro_data_endpoint(table_name: str):
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

        return {"message": f"Data for {table_name} restored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/employees_metrics/")
async def get_employees_metrics(config: dict):
    """
    Number of employees hired for each job and department in "year"
    divided by quarter, ordered alphabetically by department and job.
    """
    file_name = config["file_name"]
    year = config["year"]
    query = sqlalchemy.text(
        f"""
            SELECT
                department,
                job,
                COUNT(*) FILTER (WHERE DATE_PART('quarter', datetime::TIMESTAMP) = 1) AS Q1,
                COUNT(*) FILTER (WHERE DATE_PART('quarter', datetime::TIMESTAMP) = 2) AS Q2,
                COUNT(*) FILTER (WHERE DATE_PART('quarter', datetime::TIMESTAMP) = 3) AS Q3,
                COUNT(*) FILTER (WHERE DATE_PART('quarter', datetime::TIMESTAMP) = 4) AS Q4
            FROM
                hired_employees h
            JOIN departments d ON h.department_id = d.id
            JOIN jobs j ON h.job_id = j.id
            WHERE
                EXTRACT(YEAR FROM datetime::DATE) = 2021
            GROUP BY
                department,
                job
            ORDER BY
                department ASC,
                job ASC;
        """
    )
    data = execute_query(query)
    pd.DataFrame.from_records(
        data, columns=["department", "job", "Q1", "Q2", "Q3", "Q4"]
    ).to_csv(file_name, index=None)
    return {"message": f"Employees metrics for {year} saved to {file_name}"}


@app.post("/department_metrics/")
async def get_department_metrics(config: dict):
    year = config["year"]
    file_name = config["file_name"]
    query = sqlalchemy.text(
        f"""
            WITH department_stats AS (
                SELECT
                    d.id,
                    COUNT(*) AS total_employees
                FROM
                    hired_employees h
                JOIN departments d ON h.department_id = d.id
                WHERE DATE_PART('year', datetime::TIMESTAMP) = 2021
                GROUP BY d.id
            ),
            mean_stats AS (
                SELECT
                    AVG(total_employees) AS mean_employees
                FROM
                    department_stats
            )
            SELECT
                d.id,
                d.department,
                ds.total_employees
            FROM
                department_stats ds
            JOIN
                departments d ON ds.id = d.id
            JOIN
                mean_stats ms ON 1 = 1
            WHERE
                ds.total_employees > ms.mean_employees
            ORDER BY
                ds.total_employees DESC;
        """
    )
    data = execute_query(query)
    df = pd.DataFrame.from_records(
        data, columns=["id", "department", "total_employees"]
    )
    df.to_csv(file_name, index=None)
    return {"message": f"Department metrics for {year} saved to {file_name}"}


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
        f"{api_url}/restore-avro-data/departments/"
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
