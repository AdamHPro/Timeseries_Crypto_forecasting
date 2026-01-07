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
