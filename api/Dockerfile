# ./api/Dockerfile

FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Launch FastAPI with Uvicorn on port 8000
# --host 0.0.0.0 is MANDATORY inside Docker to be accessible from outside
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]