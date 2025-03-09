# Use the official Python 3.12 image
FROM python:3.12

# Set environment variables to avoid buffering issues
ENV PYTHONUNBUFFERED=1

# Install necessary dependencies
RUN pip install --no-cache-dir pandas google-cloud-bigquery pandas-gbq requests pyarrow python-dotenv

# Set the working directory inside the container
WORKDIR /app

# Copy the service account key file from the build context
COPY keys/bigq-dbt.json /app/bigq-dbt.json

# Copy all required files (including .env, if needed)
COPY . .

# Set Google Application Credentials insid
