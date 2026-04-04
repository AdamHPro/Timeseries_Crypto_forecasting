# Time Series Crypto Forecasting

End-to-end crypto forecasting project with:
- an ETL and training pipeline,
- a FastAPI backend,
- a React frontend,
- PostgreSQL,
- and Airflow orchestration.

The pipeline fetches BTC/USD market data, updates the database, trains an XGBoost model, and exposes the latest prediction through the API.

## Architecture

1. **ETL + Training (`etl/`)**
   - Fetches new BTC/USD data.
   - Stores parquet files in `data_lake/`.
   - Updates PostgreSQL.
   - Trains model and saves artifacts in `shared_models/`.

2. **API (`api/`)**
   - Serves health and prediction endpoints.
   - Reads latest model prediction from PostgreSQL.

3. **Frontend (`front/`)**
   - Calls the API and displays prediction data.

4. **Airflow (`airflow/`)**
   - Schedules and orchestrates the daily ETL/training pipeline.

## Tech Stack

- **Backend:** FastAPI (Python)
- **Pipeline:** Python ETL + XGBoost
- **Frontend:** React
- **Orchestration:** Apache Airflow
- **Database:** PostgreSQL
- **Containerization:** Docker + Docker Compose

## Quick Start

### Prerequisites

- Docker
- Docker Compose

### 1) Clone the repository

```bash
git clone https://github.com/AdamHPro/Timeseries_Crypto_forecasting.git
cd Timeseries_Crypto_forecasting
```

### 2) Configure environment variables

```bash
cp .env.example .env
```

Default variables in `.env.example`:
- `DB_HOST=localhost`
- `DB_PORT=5433`
- `DB_NAME=postgres`
- `DB_USER=postgres`
- `DB_PASS=postgres`
- `BACKEND_CORS_ORIGINS=http://localhost:3000`
- `REACT_APP_API_URL=http://localhost:8000`

### 3) Build and start all services

```bash
docker-compose up --build
```

### 4) Access services

- Frontend: `http://localhost:3000`
- FastAPI: `http://localhost:8000`
- FastAPI docs: `http://localhost:8000/docs`
- Airflow UI: `http://localhost:8080` (default: `admin` / `admin`)
- Main PostgreSQL (project DB): `localhost:5433`

## Main API Endpoints (for the FrontEnd)

- `GET /health` → service status
- `GET /predictions/latest` → latest prediction record

## Project Structure

- `api/` FastAPI application
- `etl/` ETL, data update, and model training code
- `front/` React frontend
- `airflow/` Airflow DAGs and config
- `data_lake/` parquet datasets
- `shared_models/` trained model artifacts
- `docker-compose.yml` multi-service local environment

## Notes

- The Airflow DAG (`mon_premier_etl_moderne`) is scheduled daily.
- Model artifacts are shared between ETL and API through `shared_models/`.
