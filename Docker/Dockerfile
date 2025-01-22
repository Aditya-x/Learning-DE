FROM python:3.12

RUN pip install pandas sqlalchemy psycopg2 requests pyarrow

WORKDIR /app

COPY ingest-data.py ingest-data.py

ENTRYPOINT ["python", "ingest-data.py"]