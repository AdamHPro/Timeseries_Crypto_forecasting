# Use the official Airflow image as a base
FROM apache/airflow:2.8.1-python3.11

# Copy the requirements file into the container
COPY requirements.txt /requirements.txt

# Upgrade pip and install the required packages
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt