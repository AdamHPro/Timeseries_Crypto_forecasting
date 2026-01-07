import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor

from config import get_db_config, get_origins


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_config = get_db_config()


class PredictionResponse(BaseModel):
    id: int
    value: float
    model_version: str
    created_at: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: You could check DB connection here
    logger.info("API is starting up...")
    yield
    # Shutdown: Clean up resources if needed
    logger.info("API is shutting down...")

app = FastAPI(
    title="Prediction API",
    description="API to serve machine learning predictions to the frontend.",
    version="1.0.0",
    lifespan=lifespan
)

origins = get_origins()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    """
    Simple health check to ensure the API is running.
    """
    return {"status": "ok", "message": "API is online"}


@app.get("/predictions/latest", response_model=PredictionResponse)
def get_latest_prediction():
    """
    Fetches the most recent prediction from the database.
    """
    query = """
        SELECT id, predicted_return_pct as value, model_version, created_at 
        FROM predictions 
        ORDER BY created_at DESC 
        LIMIT 1;
    """

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            result = cursor.fetchone()

            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="No predictions found in the database."
                )

            # Convert timestamp to string for JSON serialization compatibility
            result['created_at'] = str(result['created_at'])

            return result

    except psycopg2.Error as e:
        logger.error(f"Database query error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Database Error"
        )
    finally:
        if conn:
            conn.close()
