from airflow.models import Variable

# API Configuration
API_KEY = Variable.get("openweather_api_key")
BASE_URL = Variable.get("openweather_base_url", 
                       default="https://api.openweathermap.org/data/2.5/weather")
CITIES = Variable.get("cities", 
                     deserialize_json=True, 
                     default=['paris', 'london', 'washington'])

# Default parameters for API requests
DEFAULT_PARAMS = {
    'units': 'metric',  # Use Celsius for temperature
    'appid': API_KEY
}

# Paths
RAW_DATA_PATH = "/app/raw_files"
CLEAN_DATA_PATH = "/app/clean_data"