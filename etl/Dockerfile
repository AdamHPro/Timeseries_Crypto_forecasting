# ./etl/Dockerfile

# Base image
FROM python:3.9-slim

WORKDIR /app

# Install dependencies first (caching mechanism)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Command to run your script
CMD ["python", "main.py"]