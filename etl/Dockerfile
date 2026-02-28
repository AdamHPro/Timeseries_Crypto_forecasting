# etl/Dockerfile

FROM python:3.11.0-slim

WORKDIR /app

# --- FIX : Installation des dépendances système (C++ compilers) ---
# Indispensable pour compiler Pandas, XGBoost et Psycopg2
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    gcc \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copie des requirements
COPY requirements.txt .

# Mise à jour de pip (souvent nécessaire pour les "wheels" modernes)
RUN pip install --upgrade pip

# Installation des librairies Python
RUN pip install --no-cache-dir -r requirements.txt

# Copie du reste du code
COPY . .
