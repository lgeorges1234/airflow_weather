import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def get_config(key, default=None, deserialize_json=False):
    try:
        from airflow.models import Variable
        return Variable.get(key, default=default, deserialize_json=deserialize_json)
    except ImportError:
        # If not in Airflow environment, use environment variables
        # Docker-compose sets variables as AIRFLOW_VAR_OPENWEATHER_API_KEY
        env_key = f'AIRFLOW_VAR_{key.upper()}'  # Match docker-compose case
        value = os.environ.get(env_key, default)
        if deserialize_json and value:
            import json
            return json.loads(value)
        return value

# API Configuration
API_KEY = get_config("openweather_api_key", os.environ.get('OPENWEATHER_API_KEY'))
BASE_URL = get_config("OPENWEATHER_BASE_URL", 
                     default="https://api.openweathermap.org/data/2.5/weather")
CITIES = get_config("CITIES", 
                   default='["paris", "london", "washington"]',
                   deserialize_json=True)

# Default parameters for API requests
DEFAULT_PARAMS = {
    'units': 'metric',  # Use Celsius for temperature
    'appid': API_KEY
}

# Set up app directory structure
if os.path.exists('/app'):
    # We're in Docker
    APP_PATH = '/app'
else:
    # We're in local development
    APP_PATH = os.path.join(Path(__file__).parents[3], 'airflow/app')  # ~/airflow/app

# Define data directories
RAW_DATA_PATH = os.path.join(APP_PATH, 'raw_files')
CLEAN_DATA_PATH = os.path.join(APP_PATH, 'clean_data')

# Ensure directories exist
os.makedirs(APP_PATH, exist_ok=True)
os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(CLEAN_DATA_PATH, exist_ok=True)

print(f"Using APP_PATH: {APP_PATH}")
print(f"Data paths: RAW_DATA_PATH={RAW_DATA_PATH}, CLEAN_DATA_PATH={CLEAN_DATA_PATH}")