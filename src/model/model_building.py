# src/model/model_building.py

import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report
import joblib

def load_and_prepare_data(data_path):
    """
    Load the processed data from CSV and create a binary churn target.

    Assumptions:
    - The processed data contains a one-hot encoded column named 'STATUS_TERMINATED'
      where a value of 1 (or True) indicates that the employee has churned (terminated),
      and 0 (or False) indicates that the employee is active.

    If 'STATUS_TERMINATED' is not found, the function will exit with a message.
    
    Args:
        data_path (str): Path to the processed CSV data.
        
    Returns:
        X (pd.DataFrame): Feature matrix.
        y (pd.Series): Binary target vector for churn.
    """
    df = pd.read_csv(data_path)
    print("=== Processed Data Loaded ===")
    print(df.info())
    print(df.head())

    # Create binary target 'churn'
    if 'STATUS_TERMINATED' in df.columns:
        try:
            df['STATUS_TERMINATED'] = df['STATUS_TERMINATED'].astype(int)
        except Exception as e:
            print("Error converting STATUS_TERMINATED to int:", e)
            return None, None
        df['churn'] = df['STATUS_TERMINATED']  # 1 means churned, 0 means active.
        df.drop(columns=['STATUS_TERMINATED'], inplace=True)
    else:
        print("Target column 'STATUS_TERMINATED' not found in the dataset.")
        return None, None

    # Separate features and target.
    y = df['churn']
    X = df.drop(columns=['churn'])
    return X, y

def train_and_evaluate_model(X, y):
    """
    Split the data into training and testing sets, train two models,
    evaluate them, and select the best model based on F1 score.

    Args:
        X (pd.DataFrame): Feature matrix.
        y (pd.Series): Target vector.

    Returns:
        best_model: The trained model with the best performance.
        metrics: A dictionary containing the performance metrics for both models.
    """
    # Split the data (80% train, 20% test)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Initialize models
    lr_model = LogisticRegression(max_iter=1000, random_state=42)
    rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
    
    # Train models
    lr_model.fit(X_train, y_train)
    rf_model.fit(X_train, y_train)
    
    # Make predictions
    y_pred_lr = lr_model.predict(X_test)
    y_pred_rf = rf_model.predict(X_test)
    
    # Evaluate using accuracy, precision, recall, and F1 score.
    metrics_lr = {
        "accuracy": accuracy_score(y_test, y_pred_lr),
        "precision": precision_score(y_test, y_pred_lr, zero_division=0),
        "recall": recall_score(y_test, y_pred_lr, zero_division=0),
        "f1_score": f1_score(y_test, y_pred_lr, zero_division=0)
    }
    metrics_rf = {
        "accuracy": accuracy_score(y_test, y_pred_rf),
        "precision": precision_score(y_test, y_pred_rf, zero_division=0),
        "recall": recall_score(y_test, y_pred_rf, zero_division=0),
        "f1_score": f1_score(y_test, y_pred_rf, zero_division=0)
    }
    
    print("=== Logistic Regression Performance ===")
    print(metrics_lr)
    print("\nClassification Report (Logistic Regression):")
    print(classification_report(y_test, y_pred_lr, zero_division=0))
    
    print("\n=== Random Forest Performance ===")
    print(metrics_rf)
    print("\nClassification Report (Random Forest):")
    print(classification_report(y_test, y_pred_rf, zero_division=0))
    
    # Select the best model based on F1 score.
    if metrics_rf["f1_score"] >= metrics_lr["f1_score"]:
        best_model = rf_model
        best_model_name = "Random Forest"
    else:
        best_model = lr_model
        best_model_name = "Logistic Regression"
    
    print(f"\nBest model selected: {best_model_name}")
    
    return best_model, {"Logistic Regression": metrics_lr, "Random Forest": metrics_rf}

def save_model(model, output_path):
    """
    Save the trained model to disk using joblib.
    
    Args:
        model: Trained scikit-learn model.
        output_path (str): File path to save the model.
    """
    # Ensure the output directory exists.
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    joblib.dump(model, output_path)
    print(f"Model saved to {output_path}")

if __name__ == "__main__":
    # Compute the project root relative to this file (assumes this file is in src/model)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    
    # Define the path to the processed (clean) data.
    processed_data_path = os.path.join(project_root, "data", "processed", "clean_data.csv")
    
    # Load and prepare data (create the churn target).
    X, y = load_and_prepare_data(processed_data_path)
    if X is None or y is None:
        print("Data loading or target creation failed. Please check the processed dataset.")
    else:
        # Train models and evaluate performance.
        best_model, metrics = train_and_evaluate_model(X, y)
        
        # Define the output model path.
        model_output_path = os.path.join(project_root, "models", "churn_model.pkl")
        
        # Save the best model to disk.
        save_model(best_model, output_path=model_output_path)