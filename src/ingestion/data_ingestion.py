# src/ingestion/data_ingestion.py

import os
import logging
import subprocess
import shutil
from datetime import datetime
import shlex

def download_kaggle_dataset(dataset_slug, dest_folder):
    """
    Download a dataset from Kaggle using the Kaggle API into dest_folder.
    """
    try:
        os.makedirs(dest_folder, exist_ok=True)
        
        # Log the start of the download.
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logging.info(f"{timestamp}: Starting download for dataset: {dataset_slug}")
        
        # Use shlex.quote to properly quote the dest_folder in case it contains spaces.
        quoted_dest = shlex.quote(dest_folder)
        # Build and run the download command.
        command = f"kaggle datasets download -d {dataset_slug} -p {quoted_dest} --unzip"
        subprocess.run(command, shell=True, check=True)
        
        # Log successful download.
        logging.info(f"{timestamp}: Successfully downloaded dataset {dataset_slug} to {dest_folder}")
    except subprocess.CalledProcessError as e:
        logging.error(f"{timestamp}: Failed to download dataset {dataset_slug}. Error: {e.stderr if e.stderr else str(e)}")
    except Exception as e:
        logging.error(f"{timestamp}: An unexpected error occurred: {e}")

def store_data(raw_folder, stored_base_folder):
    """
    Copy downloaded files from the raw folder into the stored folder structure.
    
    The stored structure will be:
       stored_base_folder/<source>/<year>/<month>/<day>/
    
    Each file name will be appended with the current timestamp.
    
    After copying, this function deletes any file directly in the 
    stored_base_folder (non-directory items) to ensure only timestamped versions remain.
    
    Importantly, if the day's target folder already exists, it is left intact so that 
    multiple DAG runs on the same day will accumulate new timestamped files.
    """
    import re

    # Get current timestamp details.
    timestamp = datetime.now()
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    time_str = timestamp.strftime("%Y%m%d_%H%M%S")
    
    # Define the target folder structure.
    # We assume "kaggle" as the source name.
    target_folder = os.path.join(stored_base_folder, "kaggle", year, month, day)
    os.makedirs(target_folder, exist_ok=True)  # DO NOT remove the folder if it exists.
    
    # Copy each file from the raw folder into the target folder with a timestamp appended.
    for file in os.listdir(raw_folder):
        file_path = os.path.join(raw_folder, file)
        if os.path.isfile(file_path):
            name, ext = os.path.splitext(file)
            new_file_name = f"{name}_{time_str}{ext}"
            target_file = os.path.join(target_folder, new_file_name)
            shutil.copy(file_path, target_file)
            logging.info(f"Stored file {target_file}")
    
    # Clean up: remove any files directly under stored_base_folder (non-directory items)
    for item in os.listdir(stored_base_folder):
        item_path = os.path.join(stored_base_folder, item)
        if os.path.isfile(item_path):
            os.remove(item_path)
            logging.info(f"Deleted non-timestamp file {item_path}")

if __name__ == "__main__":
    # Compute the project root
    script_path = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(script_path, "../../"))
    
    # Define the raw destination folder (without any timestamp subfolder)
    raw_folder = os.path.join(project_root, "data", "raw", "kaggle")
    os.makedirs(raw_folder, exist_ok=True)
    
    # Configure logging.
    logs_path = os.path.join(project_root, "logs")
    os.makedirs(logs_path, exist_ok=True)
    logging.basicConfig(
        filename=os.path.join(logs_path, 'ingestion.log'),
        level=logging.INFO,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )
    
    # Download the dataset to the raw folder.
    dataset_slug = "HRAnalyticRepository/employee-attrition-data"
    download_kaggle_dataset(dataset_slug, raw_folder)
    
    # Now copy the downloaded file(s) into the stored folder structure.
    stored_folder = os.path.join(project_root, "data", "stored", "raw")
    store_data(raw_folder, stored_folder)