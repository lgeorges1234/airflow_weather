import sys
import os
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import requests
from airflow.models import Variable
import pandas as pd

# Add the parent directory to the Python path
dag_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(dag_dir)

# Define the task functions directly in the DAG file to avoid import issues
def collect_weather_data(**context):
    """
    Collect weather data from OpenWeatherMap API
    """
    try:
        # Get API key and cities from Airflow variables - matching config.py casing
        api_key = Variable.get("OPENWEATHER_API_KEY")
        try:
            cities = Variable.get("CITIES", deserialize_json=True)
        except KeyError:
            # If CITIES variable doesn't exist, use default value
            cities = ["paris", "london", "washington"]
        base_url = "https://api.openweathermap.org/data/2.5/weather"
        
        # Create timestamp for filename
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
        
        # Prepare API request
        weather_data = []
        for city in cities:
            params = {
                'q': city,
                'appid': api_key,
                'units': 'metric'
            }
            
            # Make the API request
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            weather_data.append(response.json())
            print(f"Successfully collected data for {city}")
        
        # Save data if collection was successful
        if weather_data:
            # Create filename with timestamp
            filename = f"{timestamp}.json"
            filepath = os.path.join('./app/raw_files', filename)
            
            # The directory should already exist due to Docker volume mounting
            if not os.path.exists('./app/raw_files'):
                raise Exception("Directory ./app/raw_files does not exist. Check Docker volume mounting.")
            
            # Save to file
            with open(filepath, 'w') as f:
                json.dump(weather_data, f)
                
            print(f"Saved weather data to {filepath}")
            return filepath
            
    except Exception as e:
        print(f"Error in collect_weather_data: {str(e)}")
        raise

def transform_recent_data(**context):
    """Transform the 20 most recent files into data.csv"""
    try:
        def transform_data_into_csv(n_files=None, filename='data.csv'):
            parent_folder = './app/raw_files'
            files = sorted(os.listdir(parent_folder), reverse=True)
            if n_files:
                files = files[:n_files]

            dfs = []
            for f in files:
                with open(os.path.join(parent_folder, f), 'r') as file:
                    data_temp = json.load(file)
                for data_city in data_temp:
                    dfs.append({
                        'temperature': data_city['main']['temp'],
                        'city': data_city['name'],
                        'pression': data_city['main']['pressure'],
                        'date': f.split('.')[0]
                    })

            df = pd.DataFrame(dfs)
            print('\n', df.head(10))
            df.to_csv(os.path.join('./app/clean_data', filename), index=False)
            
        transform_data_into_csv(n_files=20, filename='data.csv')
    except Exception as e:
        print(f"Error in transform_recent_data: {str(e)}")
        raise

def transform_full_data(**context):
    """Transform all files into fulldata.csv"""
    try:
        def transform_data_into_csv(n_files=None, filename='fulldata.csv'):
            parent_folder = './app/raw_files'
            files = sorted(os.listdir(parent_folder), reverse=True)
            if n_files:
                files = files[:n_files]

            dfs = []
            for f in files:
                with open(os.path.join(parent_folder, f), 'r') as file:
                    data_temp = json.load(file)
                for data_city in data_temp:
                    dfs.append({
                        'temperature': data_city['main']['temp'],
                        'city': data_city['name'],
                        'pression': data_city['main']['pressure'],
                        'date': f.split('.')[0]
                    })

            df = pd.DataFrame(dfs)
            print('\n', df.head(10))
            df.to_csv(os.path.join('./app/clean_data', filename), index=False)
            
        transform_data_into_csv(filename='fulldata.csv')
    except Exception as e:
        print(f"Error in transform_full_data: {str(e)}")
        raise

def train_linear_regression(**context):
    """Train and evaluate linear regression model"""
    from sklearn.linear_model import LinearRegression
    from sklearn.model_selection import cross_val_score
    import pandas as pd
    
    try:
        # Read and prepare data
        features, target = prepare_data()
        
        # Train and evaluate model
        model = LinearRegression()
        score = compute_model_score(model, features, target)
        
        # Push score to XCom
        context['task_instance'].xcom_push(key='lr_score', value=score)
        
    except Exception as e:
        print(f"Error in train_linear_regression: {str(e)}")
        raise

def train_decision_tree(**context):
    """Train and evaluate decision tree model"""
    from sklearn.tree import DecisionTreeRegressor
    
    try:
        # Read and prepare data
        features, target = prepare_data()
        
        # Train and evaluate model
        model = DecisionTreeRegressor()
        score = compute_model_score(model, features, target)
        
        # Push score to XCom
        context['task_instance'].xcom_push(key='dt_score', value=score)
        
    except Exception as e:
        print(f"Error in train_decision_tree: {str(e)}")
        raise

def train_random_forest(**context):
    """Train and evaluate random forest model"""
    from sklearn.ensemble import RandomForestRegressor
    
    try:
        # Read and prepare data
        features, target = prepare_data()
        
        # Train and evaluate model
        model = RandomForestRegressor()
        score = compute_model_score(model, features, target)
        
        # Push score to XCom
        context['task_instance'].xcom_push(key='rf_score', value=score)
        
    except Exception as e:
        print(f"Error in train_random_forest: {str(e)}")
        raise

def select_best_model(**context):
    """Select and save the best performing model"""
    from sklearn.linear_model import LinearRegression
    from sklearn.tree import DecisionTreeRegressor
    from sklearn.ensemble import RandomForestRegressor
    from joblib import dump
    
    try:
        # Get scores from XCom
        ti = context['task_instance']
        lr_score = ti.xcom_pull(task_ids='model_training.train_linear_regression', key='lr_score')
        dt_score = ti.xcom_pull(task_ids='model_training.train_decision_tree', key='dt_score')
        rf_score = ti.xcom_pull(task_ids='model_training.train_random_forest', key='rf_score')
        
        # Compare scores and select best model
        scores = {
            'linear_regression': (lr_score, LinearRegression()),
            'decision_tree': (dt_score, DecisionTreeRegressor()),
            'random_forest': (rf_score, RandomForestRegressor())
        }
        
        best_model_name = max(scores.items(), key=lambda x: x[1][0])[0]
        best_model = scores[best_model_name][1]
        
        # Train final model and save
        features, target = prepare_data()
        best_model.fit(features, target)
        dump(best_model, './app/clean_data/best_model.pickle')
        
        print(f"Best model ({best_model_name}) saved successfully")
        
    except Exception as e:
        print(f"Error in select_best_model: {str(e)}")
        raise

def prepare_data(path_to_data='./app/clean_data/fulldata.csv'):
    """Prepare data for model training"""
    import pandas as pd
    
    # reading data
    df = pd.read_csv(path_to_data)
    # ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)

    dfs = []

    for c in df['city'].unique():
        df_temp = df[df['city'] == c]

        # creating target
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)

        # creating features
        for i in range(1, 10):
            df_temp.loc[:, f'temp_m-{i}'] = df_temp['temperature'].shift(-i)

        # deleting null values
        df_temp = df_temp.dropna()

        dfs.append(df_temp)

    # concatenating datasets
    df_final = pd.concat(dfs, axis=0, ignore_index=False)

    # deleting date variable
    df_final = df_final.drop(['date'], axis=1)

    # creating dummies for city variable
    df_final = pd.get_dummies(df_final)

    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target

def compute_model_score(model, X, y):
    """Compute cross-validation score for a model"""
    from sklearn.model_selection import cross_val_score
    
    # computing cross val
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    model_score = cross_validation.mean()

    return model_score

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'weather_prediction_pipeline',
    default_args=default_args,
    description='Weather data collection and prediction pipeline',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['weather', 'airflow_exam']
) as dag:
    
    # Build task groups
    with TaskGroup(group_id='data_collection') as data_collection:
        collect_weather = PythonOperator(
            task_id='collect_weather_data',
            python_callable=collect_weather_data,
            provide_context=True,
            dag=dag
        )
    
    with TaskGroup(group_id='data_processing') as data_processing:
        transform_recent = PythonOperator(
            task_id='transform_recent_data',
            python_callable=transform_recent_data,
            provide_context=True,
            dag=dag
        )

        transform_full = PythonOperator(
            task_id='transform_full_data',
            python_callable=transform_full_data,
            provide_context=True,
            dag=dag
        )
    
    with TaskGroup(group_id='model_training') as model_training:
        train_lr = PythonOperator(
            task_id='train_linear_regression',
            python_callable=train_linear_regression,
            provide_context=True,
            dag=dag
        )

        train_dt = PythonOperator(
            task_id='train_decision_tree',
            python_callable=train_decision_tree,
            provide_context=True,
            dag=dag
        )

        train_rf = PythonOperator(
            task_id='train_random_forest',
            python_callable=train_random_forest,
            provide_context=True,
            dag=dag
        )

        select_model = PythonOperator(
            task_id='select_best_model',
            python_callable=select_best_model,
            provide_context=True,
            dag=dag
        )

        [train_lr, train_dt, train_rf] >> select_model
    
    # Define main workflow
    data_collection >> data_processing >> model_training