# src/ingestion/data_ingestion.py

import os
import logging
import subprocess
from datetime import datetime

def download_kaggle_dataset(dataset_slug, dest_folder):
    """
    Download a dataset from Kaggle using the Kaggle API.
    """
    try:
        # Ensure the destination folder exists (it should already have been created with the timestamp)
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
    # Determine the script's directory and compute the project root.
    script_path = os.path.dirname(__file__)  # .../src/ingestion
    project_root = os.path.abspath(os.path.join(script_path, "../../"))  # Go up two levels to the project root

    # Define the base destination folder for raw Kaggle data.
    base_dest_folder = os.path.join(project_root, "data", "raw", "kaggle")
    
    # Create a new subfolder with the current timestamp
    timestamp_folder = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_folder = os.path.join(base_dest_folder, timestamp_folder)
    
    # Configure logging
    logs_path = os.path.join(project_root, "logs")
    os.makedirs(logs_path, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(logs_path, 'ingestion.log'),
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )

    # Download the dataset into the timestamped folder
    dataset_slug = "HRAnalyticRepository/employee-attrition-data"
    download_kaggle_dataset(dataset_slug, dest_folder)