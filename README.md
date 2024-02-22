# gcp-data-api

A simple API to load historical data into GCP and provide endpoints for batch processing of new data.

## Table of Contents

- [gcp-data-api](#gcp-data-api)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Endpoints](#endpoints)
  - [Results](#results)

## Introduction

The `gcp-data-api` is a repository that contains a simple API for loading historical data into Google Cloud Platform (GCP) and providing endpoints for batch processing of new data, as well as backing up existing data and produce metrics. It aims to provide a convenient and efficient way to handle data processing tasks in a GCP environment.

## Installation

To install and set up the `gcp-data-api`, follow these steps:

1. Clone the repository: `git clone https://github.com/your-username/gcp-data-api.git`
2. Change into the project directory: `cd gcp-data-api`
3. Install the [gcloud CLI](https://cloud.google.com/sdk/docs/install)
4. Install the required dependencies: `pip install requirements.txt`

Alternatively, you can pull the docker image from GCP Artifact Registry (please contact me for IAM access using your email address). Follow these steps:

1. Authenticate to the google cloud client by running `gcloud auth login`
2. Configure Docker credentials: `gcloud auth configure-docker us-east1-docker.pkg.dev`
3. Pull the latest version of the image `docker pull us-east1-docker.pkg.dev/globant-api/globant-docker/fastapi:latest`
4. Now, you are able to start the app as such: `docker run -p 8000:8000 us-east1-docker.pkg.dev/globant-api/globant-docker/fastapi:latest`
5. Make request to your localhost as normal.

## Usage

To use the `gcp-data-api`, follow these steps:

1. Start the API server: `uvicorn main:app --host 0.0.0.0 --port 8000 --reload`
2. Access the API endpoints using the provided base URL.

## Endpoints

The `gcp-data-api` provides the following endpoints:

- `/batch-transactions/` - Processes new data in batches.
- `/backup/{table_name}/` - Backs up data from table_name into GCS.
- `restore-avro-data/{table_name}/` - Restore data backed uo in GCS.
- `/employees_metrics/` - Gets metrics on the employees in the DB.
- `/department_metrics/` - Gets metrics on the departments in the DB.

For detailed information on how to use these endpoints, refer to the API documentation.

## Results

The last endpoints of the API produce metrics that can be analyzed!
They are located in the `data_analysis` folder. 

Refer to the documentation to know where to watch a visualization of this data

