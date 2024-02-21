"""
This script is a sample of how to use the API 
to backup and restore data, and load batch transactions from a JSON file.
"""
if __name__ == "__main__":
    import requests, json
    api_url = "http://localhost:8000"

    # Backup and restore data
    response = requests.post(
        f"{api_url}/backup/departments/",
    )
    print(response.status_code)
    print(response.json())

    response = requests.post(
        f"{api_url}/restore-avro-data/departments", json=config
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
