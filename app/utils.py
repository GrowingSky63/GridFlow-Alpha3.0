from sqlalchemy import URL
import os

def load_env_if_exists():
    """
    Try to import, find and load a .env file to the env variables.
    """
    try:
        import dotenv
        dotenv_path = dotenv.find_dotenv()
        if dotenv_path:
            dotenv.load_dotenv(dotenv_path)
    except ModuleNotFoundError:
        pass

def make_url_by_environment(db_name='postgres') -> URL:
    """
    Create sqlalchemy.URL using environment or default variables.
    """
    load_env_if_exists()
    return URL.create(
        drivername=os.getenv('DB_DRIVER_NAME', 'postgresql'),
        host=os.getenv('DB_HOST', '127.0.0.1'),
        port=int(os.getenv('DB_PORT', '5432')),
        username=os.getenv('DB_USERNAME', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres'),
        database=os.getenv('DB_BDGD_NAME', db_name),
    )