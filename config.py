from db_models import *
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER
from sqlalchemy import MetaData, Table, Column
from pydantic import BaseModel, field_validator
from datetime import datetime

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


if __name__ == "__main__":
    employee = EmployeeTransaction(
        id=1,
        name="Jhon",
        datetime="2021-01-01T00:00:00",
        department_id=1,
        job_id=1,
    )
    print(employee)
    job = JobTransaction(id="1", job="Software Engineer")
    print(job)
    department = DepartmentTransaction(id="1", department="Engineering")
    print(department)


transactions = {
    "hired_employees": EmployeeTransaction,
    "departments": DepartmentTransaction,
    "jobs": JobTransaction,
}

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
