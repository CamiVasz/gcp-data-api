
import pandas as pd
def clean_data():
    # Read csv data
    employees = pd.read_csv(
        "data/hired_employees.csv",
        names=["id", "name", "datetime", "department_id", "job_id"],
    )
    departments = pd.read_csv("data/departments.csv", names=["id", "department"])
    jobs = pd.read_csv("data/jobs.csv", names=["id", "job"])

    employees.dropna(inplace=True)
    employees["id"] = employees["id"].astype(int)
    employees["department_id"] = employees["department_id"].astype(int)
    employees["job_id"] = employees["job_id"].astype(int)

    departments.dropna(inplace=True)
    departments["id"] = departments["id"].astype(int)

    jobs.dropna(inplace=True)
    jobs["id"] = jobs["id"].astype(int)

    employees.to_csv("data/hired_employees.csv", index=False, sep=",", header=None)
    departments.to_csv("data/departments.csv", index=False, sep=",", header=None)
    jobs.to_csv("data/jobs.csv", index=False, sep=",", header=None)

if __name__ == "__main__":
    clean_data()
    print("Data cleaned successfully!")
