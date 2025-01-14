from datetime import datetime
import os
from urllib import request
from weather_pipeline.config import base_url, CITIES, DEFAULT_PARAMS, RAW_DATA_PATH

def collect_weather_data():
    """
    Collecte weather data from OpenWeatherMap API for specified cities.
    Save the collected data in a JSON file
    """

    
    # Create timestamp for filename
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')

    # Prepare API request
    weather_data = []
    for city in CITIES:
        try:
            params = DEFAULT_PARAMS.copy()
            params['q'] = city

            # Make the api request
            response = request.get(base_url, params=params)
            response.raise_for_status() # Raise exception for bad status code

            # Add response to weather_data list
            weather_data.append(response.json())
            print(f"Successfully collected data for {city}")
        except request.exceptions.RequestException as e:
            print(f"Error collecting data for {city}: {str(e)}")
            continue
    
    # Save data if collection was successfull
    if weather_data:
        # Create filename with timestamp
        filename = f"{timestamp}.json"
        filepath = os.path.join(RAW_DATA_PATH, filename)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # Save to file
        with open(filepath, 'w') as f:
            json.dump(weather_data, f)
            
        print(f"Saved weather data to {filepath}")
        return filepath
    else:
        raise Exception("No weather data was collected")
