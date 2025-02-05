# src/preparation/data_preparation.py

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import LabelEncoder
from datetime import datetime

def find_latest_file(root_dir, extension=".csv"):
    """
    Recursively search for the latest file with the specified extension in root_dir.
    
    Args:
        root_dir (str): The directory to search within.
        extension (str): The file extension to look for.
        
    Returns:
        str or None: The path to the latest file found, or None if no file is found.
    """
    latest_time = 0
    latest_file = None
    for root, _, files in os.walk(root_dir):
        for file in files:
            # Skip hidden or system files (e.g., .DS_Store)
            if file.startswith('.'):
                continue
            if file.lower().endswith(extension):
                file_path = os.path.join(root, file)
                mtime = os.path.getmtime(file_path)
                if mtime > latest_time:
                    latest_time = mtime
                    latest_file = file_path
    return latest_file

def remove_outliers_iqr(df, column):
    """
    Remove outliers from a DataFrame column using the IQR method.
    
    Args:
        df (pd.DataFrame): The DataFrame.
        column (str): The name of the column to process.
        
    Returns:
        pd.DataFrame: The DataFrame with outliers removed for the specified column.
    """
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

def prepare_data(file_path):
    """
    Load, clean, and transform the raw data from the given CSV file.
    
    Processing steps include:
      - Displaying initial info and summary statistics.
      - Handling missing values.
      - Removing duplicates.
      - Removing outliers from numeric columns.
      - (Optionally) Standardizing numeric columns.
      - Processing categorical variables:
           * For columns with ≤ 20 unique values, apply one-hot encoding.
           * For columns with > 20 and ≤ 50 unique values, apply label encoding.
           * For columns with > 50 unique values, drop the column.
      - Saving the cleaned dataset.
      - Generating and saving EDA plots.
    
    Args:
        file_path (str): The path to the raw CSV file.
        
    Returns:
        pd.DataFrame: The cleaned and processed DataFrame.
    """
    # Load the dataset
    df = pd.read_csv(file_path)
    
    print("=== Initial Data Information ===")
    print(df.info())
    print("\n=== First 5 Rows ===")
    print(df.head())
    
    # --- Step 1: Drop Non-Predictive Columns ---
    # Drop identifiers and raw date columns that we won't use for modeling.
    cols_to_drop = ['EmployeeID', 'recorddate_key', 'birthdate_key', 'orighiredate_key', 'terminationdate_key']
    df.drop(columns=[col for col in cols_to_drop if col in df.columns], inplace=True)
    
    # --- Step 2: Handle Missing Values ---
    missing_values = df.isnull().sum()
    print("\n=== Missing Values Per Column ===")
    print(missing_values[missing_values > 0])
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64']:
            df[col].fillna(df[col].median(), inplace=True)
        else:
            df[col].fillna(df[col].mode()[0], inplace=True)
    
    # --- Step 3: Remove Duplicate Rows ---
    num_duplicates = df.duplicated().sum()
    print(f"\nFound {num_duplicates} duplicate rows.")
    df.drop_duplicates(inplace=True)
    
    # --- Step 4: Remove Outliers from Numeric Columns ---
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_cols:
        initial_count = df.shape[0]
        df = remove_outliers_iqr(df, col)
        final_count = df.shape[0]
        if final_count < initial_count:
            print(f"Removed {initial_count - final_count} outlier rows from '{col}'.")
    
    # --- Step 5: (Optional) Standardize Numeric Columns ---
    for col in numeric_cols:
        mean_val = df[col].mean()
        std_val = df[col].std()
        if std_val != 0:
            df[col] = (df[col] - mean_val) / std_val
        else:
            df[col] = 0
    
    # --- Step 6: Process Categorical Variables ---
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    
    one_hot_cols = []
    label_enc_cols = []
    drop_cols = []
    print("\n=== Categorical Column Cardinality ===")
    for col in categorical_cols:
        unique_count = df[col].nunique()
        print(f"Column '{col}' has {unique_count} unique values.")
        if unique_count <= 20:
            one_hot_cols.append(col)
        elif unique_count <= 50:
            label_enc_cols.append(col)
        else:
            drop_cols.append(col)
    
    if drop_cols:
        print("\nDropping high-cardinality columns:", drop_cols)
        df.drop(columns=drop_cols, inplace=True)
    
    if one_hot_cols:
        df = pd.get_dummies(df, columns=one_hot_cols, drop_first=True)
    
    if label_enc_cols:
        le = LabelEncoder()
        for col in label_enc_cols:
            df[col] = le.fit_transform(df[col])
    
    # --- Step 7: Save the Cleaned Dataset ---
    # Compute the project root to define absolute paths.
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    processed_folder = os.path.join(project_root, "data", "processed")
    os.makedirs(processed_folder, exist_ok=True)
    clean_file_path = os.path.join(processed_folder, "clean_data.csv")
    df.to_csv(clean_file_path, index=False)
    print(f"\nClean data saved to {clean_file_path}")
    
    # --- Step 8: Generate EDA Plots ---
    eda_folder = os.path.join(processed_folder, "EDA")
    os.makedirs(eda_folder, exist_ok=True)
    
    # Plot 1: Histograms with KDE for numeric columns
    if len(numeric_cols) > 0:
        plt.figure(figsize=(5 * len(numeric_cols), 4))
        for i, col in enumerate(numeric_cols, 1):
            plt.subplot(1, len(numeric_cols), i)
            sns.histplot(df[col], kde=True, bins=30)
            plt.title(f"Distribution of {col}")
        hist_path = os.path.join(eda_folder, "numeric_histograms.png")
        plt.tight_layout()
        plt.savefig(hist_path)
        plt.close()
        print(f"Numeric histograms saved to: {hist_path}")
    
    # Plot 2: Boxplots for numeric columns
    if len(numeric_cols) > 0:
        plt.figure(figsize=(5 * len(numeric_cols), 4))
        for i, col in enumerate(numeric_cols, 1):
            plt.subplot(1, len(numeric_cols), i)
            sns.boxplot(x=df[col])
            plt.title(f"Boxplot of {col}")
        boxplot_path = os.path.join(eda_folder, "numeric_boxplots.png")
        plt.tight_layout()
        plt.savefig(boxplot_path)
        plt.close()
        print(f"Numeric boxplots saved to: {boxplot_path}")
    
    # Plot 3: Correlation Heatmap (if more than one numeric column)
    if len(numeric_cols) > 1:
        plt.figure(figsize=(10, 8))
        corr_matrix = df[numeric_cols].corr()
        sns.heatmap(corr_matrix, annot=True, cmap="coolwarm", fmt=".2f")
        heatmap_path = os.path.join(eda_folder, "correlation_heatmap.png")
        plt.tight_layout()
        plt.savefig(heatmap_path)
        plt.close()
        print(f"Correlation heatmap saved to: {heatmap_path}")
    
    return df

if __name__ == "__main__":
    # Compute project root.
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    
    # Use the stored folder for raw data.
    raw_data_dir = os.path.join(project_root, "data", "stored", "raw", "kaggle")
    
    latest_file = find_latest_file(raw_data_dir, extension=".csv")
    
    if not latest_file:
        print("No CSV file found in:", raw_data_dir)
    else:
        print("Preparing data from file:", latest_file)
        prepare_data(latest_file)