# src/transformation/data_transformation.py

import os
import pandas as pd
import sqlite3

def transform_data(df):
    """
    Perform feature engineering and transformations on the clean DataFrame.
    
    Key changes in this edited version:
      - Drops non-predictive identifier and date columns.
      - Performs sample feature engineering (e.g., creating an AgeGroup feature).
      - Standardizes numeric columns.
      - Identifies and drops high-cardinality categorical columns.
      - Applies one-hot encoding only to the remaining low-cardinality categorical columns.
    
    Args:
        df (pd.DataFrame): The clean DataFrame.
        
    Returns:
        pd.DataFrame: The transformed DataFrame with new features.
    """
    # --- Step 1: Drop Non-Predictive Columns ---
    # These columns (identifiers and raw dates) are not useful for modeling and can cause issues when transformed.
    columns_to_drop = ['EmployeeID', 'recorddate_key', 'birthdate_key', 'orighiredate_key', 'terminationdate_key']
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
    
    # --- Step 2: Feature Engineering ---
    # Create an AgeGroup feature if the 'age' column exists.
    if 'age' in df.columns:
        # Define bins and labels for age groups.
        bins = [17, 30, 45, 60, 100]
        labels = ['Young', 'Mid-age', 'Senior', 'Veteran']
        df['AgeGroup'] = pd.cut(df['age'], bins=bins, labels=labels)
    
    # --- Step 3: Standardize Numeric Columns ---
    # Identify numeric columns (e.g., age, length_of_service)
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
    for col in numeric_cols:
        mean = df[col].mean()
        std = df[col].std()
        if std != 0:
            df[col] = (df[col] - mean) / std
        else:
            df[col] = 0  # In case the column has constant values
    
    # --- Step 4: Handle Categorical Variables ---
    # Identify categorical columns (e.g., city_name, department_name, job_title, store_name, gender_short, etc.)
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    
    # Determine high- versus low-cardinality columns based on a threshold.
    high_cardinality_cols = []
    low_cardinality_cols = []
    cardinality_threshold = 50 
    print("\n=== Categorical Column Cardinality ===")
    for col in categorical_cols:
        unique_count = df[col].nunique()
        print(f"Column '{col}' has {unique_count} unique values.")
        if unique_count > cardinality_threshold:
            high_cardinality_cols.append(col)
        else:
            low_cardinality_cols.append(col)
    
    if high_cardinality_cols:
        print(f"\nDropping high-cardinality columns: {high_cardinality_cols}")
        df = df.drop(columns=high_cardinality_cols)
    else:
        print("\nNo high-cardinality columns to drop.")
    
    # Apply one-hot encoding only to the low-cardinality categorical columns.
    if low_cardinality_cols:
        df_encoded = pd.get_dummies(df, columns=low_cardinality_cols, drop_first=True)
    else:
        df_encoded = df.copy()
    
    return df_encoded

def store_transformed_data(df, db_path):
    """
    Store the transformed DataFrame into a SQLite database.
    
    The DataFrame is written into a table named "employee_features".
    
    Args:
        df (pd.DataFrame): The transformed DataFrame.
        db_path (str): The path to the SQLite database file.
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Connect to (or create) the SQLite database
    conn = sqlite3.connect(db_path)
    
    # Write the DataFrame to a SQL table named "employee_features".
    df.to_sql("employee_features", conn, if_exists="replace", index=False)
    conn.close()
    print(f"Transformed data stored in SQLite database at: {db_path}")

if __name__ == "__main__":
    # Compute the project root relative to this file (assumes file is at src/transformation)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    
    # Define the path to the cleaned CSV file produced in the data preparation step.
    clean_data_path = os.path.join(project_root, "data", "processed", "clean_data.csv")
    
    if not os.path.exists(clean_data_path):
        print(f"Clean data file not found at: {clean_data_path}")
    else:
        # Load the clean data.
        df_clean = pd.read_csv(clean_data_path)
        print("=== Loaded Clean Data ===")
        print(df_clean.info())
        print(df_clean.head())
        
        # Transform the data (perform feature engineering and encoding).
        df_transformed = transform_data(df_clean)
        print("\n=== Transformed Data Information ===")
        print(df_transformed.info())
        print(df_transformed.head())
        
        # Define the path to store the SQLite database for the transformed data.
        db_path = os.path.join(project_root, "data", "processed", "employee_attrition_features.db")
        
        # Store the transformed data into the SQLite database.
        store_transformed_data(df_transformed, db_path)