import os


def pull_db_config():
    """
    Pulls database configuration from environment variables with default values.
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
