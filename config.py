from db_models import *
from sqlalchemy.dialects.postgresql import VARCHAR, INTEGER
from sqlalchemy import MetaData, Table, Column

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
