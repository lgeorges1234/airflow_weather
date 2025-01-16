from datetime import datetime
import json
import os
import requests
import traceback
from weather_pipeline.config import BASE_URL, CITIES, DEFAULT_PARAMS, RAW_DATA_PATH

def collect_weather_data():
    """
    Collecte weather data from OpenWeatherMap API for specified cities.
    Save the collected data in a JSON file.
    
    Returns:
        str: Path to the saved JSON file
    """
    
    # Create timestamp for filename
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')

    # Prepare API request
    weather_data = []
    errors = []

    for city in CITIES:
        try:
            params = DEFAULT_PARAMS.copy()
            params['q'] = city

            # Make the api request
            response = requests.get(BASE_URL, params=params)
            print(response.json())
            
            # Enhanced error handling
            if response.status_code == 401:
                print(f"Authentication Error for {city}: Check your API key")
                errors.append(f"{city}: Authentication Error (401)")
            elif response.status_code == 429:
                print(f"Rate Limit Exceeded for {city}: Too many requests")
                errors.append(f"{city}: Rate Limit Exceeded (429)")
            elif response.status_code != 200:
                print(f"Unexpected error for {city}: Status code {response.status_code}")
                errors.append(f"{city}: Unexpected error (Status {response.status_code})")
                continue

            response.raise_for_status()

            # Add response to weather_data list
            city_data = response.json()
            city_data['city_name'] = city  # Add city name to the data
            weather_data.append(city_data)
            print(f"Successfully collected data for {city}")
        
        except requests.exceptions.RequestException as e:
            print(f"Request Error for {city}: {str(e)}")
            errors.append(f"{city}: Request Error - {str(e)}")
            
            # Additional detailed error logging
            print("Detailed Error Traceback:")
            traceback.print_exc()
        
        except Exception as e:
            print(f"Unexpected error for {city}: {str(e)}")
            errors.append(f"{city}: Unexpected Error - {str(e)}")
            traceback.print_exc()
    
    # Save data if collection was successful
    if weather_data:
        # Create filename with timestamp
        filename = f"{timestamp}.json"
        filepath = os.path.join(RAW_DATA_PATH, filename)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        # Save to file
        with open(filepath, 'w') as f:
            json.dump({
                'weather_data': weather_data,
                'errors': errors
            }, f, indent=2)
            
        print(f"Saved weather data to {filepath}")
        
        # Log any errors that occurred
        if errors:
            print("Errors encountered during data collection:")
            for error in errors:
                print(error)
        
        return filepath
    else:
        error_message = "No weather data was collected. Errors: " + "; ".join(errors)
        raise Exception(error_message)