FROM python:3.9-slim

WORKDIR /app

COPY . /app

RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    && curl -sSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add - \
    && echo "deb http://packages.cloud.google.com/apt cloud-sdk-$(lsb_release -c -s) main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y google-cloud-sdk \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

ENV GCS_BUCKET_NAME=globant-data
ENV GCS_KEY_FILE_NAME=globant-api-f53c5352239b.json

RUN gsutil cp gs://${GCS_BUCKET_NAME}/${GCS_KEY_FILE_NAME} /app/service-account-key.json

ENV GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-key.json

EXPOSE 8000

ENV FASTAPI_APP main:app

ENV GOOGLE_CLOUD_PROJECT=globant-api

CMD ["uvicorn", "--host", "0.0.0.0", "--port", "8000", "main:app", "--reload"]
