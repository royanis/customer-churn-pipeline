# src/ingestion/data_ingestion.py

import os
import logging
import subprocess
from datetime import datetime

def download_kaggle_dataset(dataset_slug, dest_folder):
    """
    Download a dataset from Kaggle using the Kaggle API.

    Args:
        dataset_slug (str): The Kaggle dataset identifier (e.g., 'HRAnalyticRepository/employee-attrition-data').
        dest_folder (str): The local folder where the dataset will be stored.
    """
    try:
        # Ensure the destination folder exists
        os.makedirs(dest_folder, exist_ok=True)
        
        # Log the start of the download
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logging.info(f"{timestamp}: Starting download for dataset: {dataset_slug}")
        
        # Build the command to download and unzip the dataset
        command = f"kaggle datasets download -d {dataset_slug} -p {dest_folder} --unzip"
        subprocess.run(command, shell=True, check=True)
        
        # Log successful download
        logging.info(f"{timestamp}: Successfully downloaded dataset {dataset_slug} to {dest_folder}")
    except subprocess.CalledProcessError as e:
        logging.error(f"{timestamp}: Failed to download dataset {dataset_slug}. Error: {e.stderr if e.stderr else str(e)}")
    except Exception as e:
        logging.error(f"{timestamp}: An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Configure logging to store logs in the logs folder
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        filename='logs/ingestion.log',
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )

    # Define the Kaggle dataset slug for Employee Attrition Data and destination folder
    dataset_slug = "HRAnalyticRepository/employee-attrition-data"
    dest_folder = os.path.join("data", "raw", "kaggle")
    
    # Download the dataset
    download_kaggle_dataset(dataset_slug, dest_folder)