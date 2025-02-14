# src/feature_store/feature_store.py

import os
import json
import sqlite3
import pandas as pd

# Compute the project root relative to this file
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# Define the absolute path where the feature store (metadata) will be saved
FEATURE_STORE_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "feature_store.json")

def register_feature(feature_name, description, source, version):
    """
    Register a feature by storing its metadata in a JSON file.

    Args:
        feature_name (str): Name of the feature.
        description (str): A brief description of the feature.
        source (str): The origin of the feature (e.g., which transformation or raw data it comes from).
        version (str): Version of the feature.
    """
    # Load existing feature metadata if available
    if os.path.exists(FEATURE_STORE_PATH):
        with open(FEATURE_STORE_PATH, 'r') as f:
            feature_store = json.load(f)
    else:
        feature_store = {}

    # Update the feature store with the new feature
    feature_store[feature_name] = {
        "description": description,
        "source": source,
        "version": version
    }

    # Save the updated feature store back to disk.
    with open(FEATURE_STORE_PATH, 'w') as f:
        json.dump(feature_store, f, indent=4)
    print(f"Feature '{feature_name}' registered.")

def get_feature_metadata(feature_name):
    """
    Retrieve metadata for a specific feature.

    Args:
        feature_name (str): The name of the feature to retrieve.

    Returns:
        dict or None: A dictionary of the feature metadata if found, else None.
    """
    if os.path.exists(FEATURE_STORE_PATH):
        with open(FEATURE_STORE_PATH, 'r') as f:
            feature_store = json.load(f)
        return feature_store.get(feature_name)
    else:
        print("Feature store file not found. No features have been registered yet.")
        return None

def list_features():
    """
    List all registered features and their metadata.

    Returns:
        dict: A dictionary containing all registered feature metadata.
    """
    if os.path.exists(FEATURE_STORE_PATH):
        with open(FEATURE_STORE_PATH, 'r') as f:
            feature_store = json.load(f)
        return feature_store
    else:
        print("Feature store file not found. No features have been registered yet.")
        return {}

def retrieve_features(query="SELECT * FROM employee_features LIMIT 5", 
                      db_path=None):
    """
    Retrieve sample feature data from the SQLite database created during the Data Transformation step.

    Args:
        query (str): The SQL query to execute.
        db_path (str): Path to the SQLite database file. If None, defaults to the processed database in the project.
    
    Returns:
        pd.DataFrame or None: DataFrame with query results if successful; otherwise, None.
    """
    if db_path is None:
        db_path = os.path.join(PROJECT_ROOT, "data", "processed", "employee_attrition_features.db")
        
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}.")
        return None

    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

if __name__ == "__main__":
    register_feature(
        feature_name="age",
        description="Normalized age of the employee.",
        source="Transformed employee attrition data (data preparation & transformation steps).",
        version="v1.0"
    )
    register_feature(
        feature_name="length_of_service",
        description="Normalized length of service (in years).",
        source="Transformed employee attrition data (data preparation & transformation steps).",
        version="v1.0"
    )
    register_feature(
        feature_name="department_name",
        description="Encoded department of the employee.",
        source="Transformed employee attrition data (data preparation & transformation steps).",
        version="v1.0"
    )
    
    # List all registered features.
    features = list_features()
    print("\n=== Registered Features ===")
    print(json.dumps(features, indent=4))
    
    # Retrieve metadata for the 'age' feature.
    age_metadata = get_feature_metadata("age")
    print("\n=== Metadata for 'age' Feature ===")
    print(json.dumps(age_metadata, indent=4))
    
    # Retrieve sample feature data from the SQLite database.
    sample_features = retrieve_features()
    if sample_features is not None:
        print("\n=== Sample Feature Data from Database ===")
        print(sample_features.head())