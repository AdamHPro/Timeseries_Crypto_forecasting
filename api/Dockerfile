# api/Dockerfile

FROM python:3.9-slim

WORKDIR /app

# --- FIX: System Dependencies ---
# We install GCC and libraries required to compile Python packages (pandas, xgboost, postgres drivers)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for Docker caching)
COPY requirements.txt .

# Upgrade pip to handle modern binary wheels better
RUN pip install --upgrade pip

# Install Python dependencies
# If this step fails, it's usually because of a typo in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Launch FastAPI
# --host 0.0.0.0 is mandatory to expose the server outside the container
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]