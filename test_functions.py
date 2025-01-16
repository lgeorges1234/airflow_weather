import os
import sys

# Add the dags directory to Python path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'dags'))

from weather_pipeline.tasks.data_collection import collect_weather_data
from weather_pipeline.tasks.data_transformation import transform_recent_data, transform_full_data
from weather_pipeline.tasks.model_training import (
    train_linear_regression,
    train_decision_tree,
    train_random_forest,
    select_best_model
)

def test_data_collection():
    """Test the data collection function"""
    print("Testing data collection...")
    try:
        # You'll need to set these environment variables
        os.environ['AIRFLOW_VAR_OPENWEATHER_API_KEY']
        os.environ['AIRFLOW_VAR_cities'] = '["paris", "london", "washington"]'
        
        result = collect_weather_data()
        print(f"Successfully collected data: {result}")
    except Exception as e:
        print(f"Error collecting data: {e}")

def test_data_transformation():
    """Test the data transformation functions"""
    print("\nTesting data transformation...")
    try:
        # Test transform_recent_data
        recent_result = transform_recent_data()
        print(f"Successfully transformed recent data: {recent_result}")
        
        # Test transform_full_data
        full_result = transform_full_data()
        print(f"Successfully transformed full data: {full_result}")
    except Exception as e:
        print(f"Error transforming data: {e}")
        raise

def test_model_training():
    """Test the model training functions"""
    print("\nTesting model training...")
    try:
        # Create a mock context for XCom
        mock_context = {
            'task_instance': type('obj', (object,), {
                'xcom_push': lambda key, value: print(f"Would push to XCom: {key}={value}"),
                'xcom_pull': lambda task_ids, key: -1.0  # Mock score
            })
        }
        
        lr_model = train_linear_regression(**mock_context)
        print(f"Trained linear regression model: {lr_model}")
        
        dt_model = train_decision_tree(**mock_context)
        print(f"Trained decision tree model: {dt_model}")
        
        rf_model = train_random_forest(**mock_context)
        print(f"Trained random forest model: {rf_model}")
        
        select_best_model(**mock_context)
    except Exception as e:
        print(f"Error training models: {e}")

if __name__ == "__main__":
    # Test individual components
    test_data_collection()
    test_data_transformation()
    test_model_training()

# # Create virtual environment
# python -m venv venv

# # Activate it (on Linux/Mac)
# source venv/bin/activate
# # OR on Windows
# # venv\Scripts\activate

# # Install requirements
# pip install -r requirements.txt

# # Test just data collection
# python -c "from test_functions import test_data_collection; test_data_collection()"

# # Test just transformation
# python -c "from test_functions import test_data_transformation; test_data_transformation()"

# # Test just model training
# python -c "from test_functions import test_model_training; test_model_training()"

# python test_functions.py