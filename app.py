from fastapi import FastAPI, HTTPException
from sqlalchemy.sql.expression import bindparam
from sqlalchemy import Table, Column, MetaData
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

def insert_batch_data(table_name, batch_data):
    # Insert data into each table based on the table name
    # Three options available: hired_employees, departments, jobs
    conn = connect_with_connector().connect()
    sql_metadata = MetaData()
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
    else:
        statement = Jobs.insert().values({
            "id": bindparam("id"),
            "job": bindparam("job")
            }
        )
    conn.execute(statement, batch_data)
    conn.commit()
    conn.close()


@app.post("/batch-transactions/")
def create_batch_transactions(batch_transaction):
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

if __name__ == "__main__":
    import requests, json
    api_url = "http://localhost:8000"

    with open("batch_transactions.json", "r") as file:
        batch_transaction = json.load(file)
        
    print(create_batch_transactions(batch_transaction))
