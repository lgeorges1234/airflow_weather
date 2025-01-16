import sys
import pandas as pd
import numpy as np
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
import joblib
import os
from typing import Tuple, Any
import argparse
from weather_pipeline.config import CLEAN_DATA_PATH

def prepare_data(path_to_data: str = os.path.join(CLEAN_DATA_PATH, 'fulldata.csv')) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Prepare the data for model training.
    
    Args:
        path_to_data: Path to the CSV file containing the full dataset
        
    Returns:
        Tuple containing features DataFrame and target Series
    """
    # Reading data
    df = pd.read_csv(path_to_data)
    
    # Ordering data according to city and date
    df = df.sort_values(['city', 'date'], ascending=True)
    
    dfs = []
    
    for c in df['city'].unique():
        df_temp = df[df['city'] == c]
        
        # Creating target (next temperature)
        df_temp.loc[:, 'target'] = df_temp['temperature'].shift(1)
        
        # Creating features (previous temperatures)
        for i in range(1, 10):
            df_temp.loc[:, f'temp_m-{i}'] = df_temp['temperature'].shift(-i)
            
        # Deleting null values
        df_temp = df_temp.dropna()
        dfs.append(df_temp)
    
    # Concatenating datasets
    df_final = pd.concat(dfs, axis=0, ignore_index=False)
    
    # Deleting date variable
    df_final = df_final.drop(['date'], axis=1)
    
    # Creating dummies for city variable
    df_final = pd.get_dummies(df_final)
    
    # Separating features and target
    features = df_final.drop(['target'], axis=1)
    target = df_final['target']
    
    return features, target

def compute_model_score(model: Any, X: pd.DataFrame, y: pd.Series, **context) -> float:
    """
    Compute cross-validation score for a model.
    
    Args:
        model: Scikit-learn model object
        X: Features DataFrame
        y: Target Series
        context: Airflow context for XCom
        
    Returns:
        float: Mean cross-validation score
    """
    # Scale the features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Computing cross validation
    cross_validation = cross_val_score(
        model,
        X_scaled,
        y,
        cv=3,
        scoring='neg_mean_squared_error'
    )
    
    model_score = cross_validation.mean()
    
    # Push score to XCom if we're in an Airflow task
    task_instance = context.get('task_instance')
    if task_instance:
        task_instance.xcom_push(
            key='model_score',
            value=float(model_score)  # Convert numpy float to Python float for JSON serialization
        )
    
    return model_score

def train_and_save_model(
    model: Any,
    X: pd.DataFrame,
    y: pd.Series,
    path_to_model: str = os.path.join(CLEAN_DATA_PATH, 'best_model.joblib')
) -> None:
    """
    Train a model and save it to disk.
    
    Args:
        model: Scikit-learn model object
        X: Features DataFrame
        y: Target Series
        path_to_model: Path where to save the model
    """
    # Scale the features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Training the model
    model.fit(X_scaled, y)
    
    # Save both the model and the scaler
    joblib.dump({
        'model': model,
        'scaler': scaler
    }, path_to_model)
    
    print(f"Model {str(model)} saved at {path_to_model}")

def train_linear_regression(**context) -> None:
    """Airflow task to train and evaluate LinearRegression"""
    X, y = prepare_data()
    model = LinearRegression()
    score = compute_model_score(model, X, y, **context)
    print(f"LinearRegression score: {score}")

def train_decision_tree(**context) -> None:
    """Airflow task to train and evaluate DecisionTreeRegressor"""
    X, y = prepare_data()
    model = DecisionTreeRegressor(random_state=42)
    score = compute_model_score(model, X, y, **context)
    print(f"DecisionTreeRegressor score: {score}")

def train_random_forest(**context) -> None:
    """Airflow task to train and evaluate RandomForestRegressor"""
    X, y = prepare_data()
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    score = compute_model_score(model, X, y, **context)
    print(f"RandomForestRegressor score: {score}")

def select_best_model(**context) -> None:
    """
    Airflow task to select and train the best performing model
    """
    task_instance = context['task_instance']
    
    # Get scores from XCom
    lr_score = task_instance.xcom_pull(task_ids='train_linear_regression', key='model_score')
    dt_score = task_instance.xcom_pull(task_ids='train_decision_tree', key='model_score')
    rf_score = task_instance.xcom_pull(task_ids='train_random_forest', key='model_score')
    
    # Compare scores (higher is better for negative MSE)
    scores = {
        'LinearRegression': (lr_score, LinearRegression()),
        'DecisionTree': (dt_score, DecisionTreeRegressor(random_state=42)),
        'RandomForest': (rf_score, RandomForestRegressor(n_estimators=100, random_state=42))
    }
    
    best_model_name = max(scores.items(), key=lambda x: x[1][0])[0]
    best_model = scores[best_model_name][1]
    
    print(f"Best model is {best_model_name} with score {scores[best_model_name][0]}")
    
    # Train and save the best model
    X, y = prepare_data()
    train_and_save_model(best_model, X, y)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', choices=['linear', 'decision', 'forest', 'select'], required=True)
    args = parser.parse_args()
    
    # Mapping of model choices to corresponding training functions
    model_functions = {
        'linear': train_linear_regression,
        'decision': train_decision_tree,
        'forest': train_random_forest,
        'select': select_best_model
    }


    # Get the corresponding function based on the model argument
    train_function = model_functions.get(args.model)

    if train_function:
        train_function()
    else:
        print(f"Invalid model: {args.model}")
        sys.exit(1)

if __name__ == '__main__':
    main()