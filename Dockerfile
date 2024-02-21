FROM python:3.9-slim

WORKDIR /app

COPY . /app

COPY ./google_cloud_credentials.json /app/google-cloud-credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/google-cloud-credentials.json

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

ENV FASTAPI_APP main:app

ENV GOOGLE_CLOUD_PROJECT=globant-api

CMD ["uvicorn", "--host", "0.0.0.0", "--port", "8000", "main:app", "--reload"]
