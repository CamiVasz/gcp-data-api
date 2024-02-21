from db_models import *
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER
from sqlalchemy import MetaData, Table, Column
from pydantic import BaseModel, field_validator
from datetime import datetime

"""
Each Transaction class is a Pydantic model that represents a transaction. 
It contains all fields for the three defined tables.

Each class also includes validation methods 
to ensure that the field values meet certain criteria.

Main functionalities
Store and validate employee transaction data.
Ensure that all ids are positive integers and not empty.
Validate that the datetime field is in the ISO datetime format.
"""

class EmployeeTransaction(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

    @field_validator("id")
    def validate_id(cls, value):
        if value <= 0:
            raise ValueError("ID must be a positive integer")
        elif not value:
            raise ValueError("ID is required")
        return value

    @field_validator("name")
    def validate_name(cls, value):
        if not value or not value.strip():
            raise ValueError("Name cannot be empty or whitespace")
        return value

    @field_validator("department_id")
    def validate_department_id(cls, value):
        if value <= 0:
            raise ValueError("Department ID must be a positive integer")
        elif not value:
            raise ValueError("ID is required")
        return value

    @field_validator("job_id")
    def validate_job_id(cls, value):
        if value <= 0:
            raise ValueError("Job ID must be a positive integer")
        elif not value:
            raise ValueError("ID is required")
        return value

    @field_validator("datetime")
    def validate_iso_datetime(cls, value):
        try:
            # Check if the provided string can be parsed as a valid ISO datetime
            datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
            return value
        except ValueError:
            raise ValueError(
                "Invalid ISO datetime format. Please use the format: YYYY-MM-DDTHH:MM:SS"
            )

class JobTransaction(BaseModel):
    id: int
    job: str

    @field_validator("id")
    def validate_id(cls, value):
        if value <= 0:
            raise ValueError("Job ID must be a positive integer")
        elif not value:
            raise ValueError("ID is required")
        return value

    @field_validator("job")
    def validate_job(cls, value):
        if not value or not value.strip():
            raise ValueError("Name cannot be empty or whitespace")
        return value

class DepartmentTransaction(BaseModel):
    id: int
    department: str

    @field_validator("id")
    def validate_id(cls, value):
        if value <= 0:
            raise ValueError("Job ID must be a positive integer")
        elif not value:
            raise ValueError("ID is required")
        return value

    @field_validator("department")
    def validate_department(cls, value):
        if not value or not value.strip():
            raise ValueError("Name cannot be empty or whitespace")
        return value

# Define a dictionary that maps table names 
# to their corresponding transaction classes
transactions = {
    "hired_employees": EmployeeTransaction,
    "departments": DepartmentTransaction,
    "jobs": JobTransaction,
}

# Define a dictionary that maps table names to 
# their corresponding SQLAlchemy table objects
tables = {"hired_employees": Table(
                                "hired_employees",
                                MetaData(),
                                Column("id", INTEGER, primary_key=True),
                                Column("name", VARCHAR(255)),
                                Column("datetime", VARCHAR(255)),
                                Column("department_id", INTEGER),
                                Column("job_id", INTEGER),
                                ),
            "departments": Table(
                                "departments",
                                MetaData(),
                                Column("id", INTEGER, primary_key=True),
                                Column("department", VARCHAR(255)),
                                ),
            "jobs": Table(
                                "jobs",
                                MetaData(),
                                Column("id", INTEGER, primary_key=True),
                                Column("job", VARCHAR(255)),
                                ),
            }
