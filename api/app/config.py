import os


def get_db_config():
    """
    Pulls database configuration from environment variables with default values.
    Returns:
        dict: A dictionary containing database connection parameters.
    """
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_NAME = os.getenv("DB_NAME", "postgres")
    DB_USER = os.getenv("DB_USER", "postgres")  # Default user
    DB_PASS = os.getenv("DB_PASS", "postgres")
    DB_PORT = os.getenv("DB_PORT", "5433")

    return {
        "host": DB_HOST,
        "name": DB_NAME,
        "user": DB_USER,
        "pass": DB_PASS,
        "port": DB_PORT
    }


def get_origins():
    """
    Returns a list of allowed CORS origins.
    Returns:
        list: A list of allowed origins for CORS.
    """
    return [
        os.getenv("REACT_API_URL"),
    ]
