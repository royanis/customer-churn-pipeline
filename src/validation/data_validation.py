# src/validation/data_validation.py

import pandas as pd
import os
from datetime import datetime

def find_latest_file(root_dir, extension=".csv"):
    """
    Recursively search for the latest file with the specified extension in root_dir.
    """
    latest_time = 0
    latest_file = None

    for root, _, files in os.walk(root_dir):
        for file in files:
            if file.lower().endswith(extension):
                file_path = os.path.join(root, file)
                mtime = os.path.getmtime(file_path)
                if mtime > latest_time:
                    latest_time = mtime
                    latest_file = file_path
    return latest_file

def validate_data(file_path):
    """
    Validate the data quality of the CSV file.
    """
    report = {}
    try:
        df = pd.read_csv(file_path)
        report['total_rows'] = df.shape[0]
        report['total_columns'] = df.shape[1]
        missing_values = df.isnull().sum()
        report['missing_values'] = missing_values[missing_values > 0].to_dict()
        duplicate_count = df.duplicated().sum()
        report['duplicate_rows'] = duplicate_count
        if 'Age' in df.columns:
            invalid_age_rows = df[(df['Age'] < 18) | (df['Age'] > 65)]
            report['invalid_age_rows'] = invalid_age_rows.shape[0]
        if 'EmployeeNumber' in df.columns:
            unique_count = df['EmployeeNumber'].nunique()
            total_count = df.shape[0]
            report['employee_number_unique'] = (unique_count == total_count)
    except Exception as e:
        report['error'] = str(e)
    return report

if __name__ == "__main__":
    # Compute project root (assumes this file is in src/validation)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    
    # Now point to the stored folder structure.
    root_storage_dir = os.path.join(project_root, "data", "stored", "raw", "kaggle")
    
    # Find the latest CSV file in the stored folder.
    latest_file = find_latest_file(root_storage_dir, extension=".csv")
    
    if not latest_file:
        print("No CSV file found in:", root_storage_dir)
    else:
        print(f"Validating the latest file: {latest_file}")
        quality_report = validate_data(latest_file)
        print("\nData Quality Report:")
        for key, value in quality_report.items():
            print(f"{key}: {value}")
        # Optionally, save the report.
        report_df = pd.DataFrame([quality_report])
        os.makedirs("logs", exist_ok=True)
        report_path = os.path.join("logs", "data_quality_report.csv")
        report_df.to_csv(report_path, index=False)
        print(f"\nData quality report saved to {report_path}")