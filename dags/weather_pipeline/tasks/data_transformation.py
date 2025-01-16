import pandas as pd
import argparse
import json
import os
from typing import Optional
from weather_pipeline.config import RAW_DATA_PATH, CLEAN_DATA_PATH

def transform_data_into_csv(n_files: Optional[int] = None, filename: str = 'data.csv') -> str:
    """
    Transform JSON weather data files into a CSV file.
    
    Args:
        n_files: Optional number of most recent files to process. If None, process all files.
        filename: Name of the output CSV file.
        
    Returns:
        str: Path to the created CSV file
    """
    # Ensure directory exists
    os.makedirs(CLEAN_DATA_PATH, exist_ok=True)
    
    # Get list of files and sort by name (which includes timestamp)
    files = sorted(os.listdir(RAW_DATA_PATH), reverse=True)
    
    # Filter to requested number of files if specified
    if n_files:
        files = files[:n_files]
    
    dfs = []
    
    # Process each JSON file
    for f in files:
        if not f.endswith('.json'):
            continue
            
        with open(os.path.join(RAW_DATA_PATH, f), 'r') as file:
            data_temp = json.load(file)
            
        for data_city in data_temp:
            dfs.append({
                'temperature': data_city['main']['temp'],
                'city': data_city['name'],
                'pression': data_city['main']['pressure'],
                'date': f.split('.')[0]
            })
    
    # Create dataframe and save to CSV
    df = pd.DataFrame(dfs)
    output_path = os.path.join(CLEAN_DATA_PATH, filename)
    df.to_csv(output_path, index=False)
    
    print(f'\nProcessed {len(files)} files. Sample of data:')
    print(df.head(10))
    print(f'\nOutput_path : {output_path}')
    
    return output_path

def transform_recent_data() -> str:
    """
    Transform the 20 most recent weather data files into CSV.
    
    Returns:
        str: Path to the created CSV file
    """
    return transform_data_into_csv(n_files=20, filename='data.csv')

def transform_full_data() -> str:
    """
    Transform all weather data files into CSV.
    
    Returns:
        str: Path to the created CSV file
    """
    return transform_data_into_csv(filename='fulldata.csv')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['recent', 'full'])
    args = parser.parse_args()
    
    if args.mode == 'recent':
        transform_recent_data()
    else:
        transform_full_data()

if __name__ == '__main__':
    main()