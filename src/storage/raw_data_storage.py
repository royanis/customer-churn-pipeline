# src/storage/raw_data_storage.py

import os
import shutil
from datetime import datetime

def partition_raw_data(source_folder, storage_root):
    """
    Organizes files from source_folder into a partitioned folder structure in storage_root.
    The partitioning is based on the file's last modification time.
    
    Folder structure:
    storage_root/<source>/<year>/<month>/<day>/<filename>
    
    Args:
        source_folder (str): The folder where raw files are initially downloaded.
        storage_root (str): The root folder where files will be organized.
    """
    # Ensure the storage root exists
    os.makedirs(storage_root, exist_ok=True)
    
    # Iterate over each file in the source folder
    for filename in os.listdir(source_folder):
        source_file_path = os.path.join(source_folder, filename)
        if os.path.isfile(source_file_path):
            # Use the file's modification time as a proxy for the timestamp
            mod_time = os.path.getmtime(source_file_path)
            file_datetime = datetime.fromtimestamp(mod_time)
            
            # Build the partition path based on the timestamp
            year = file_datetime.strftime("%Y")
            month = file_datetime.strftime("%m")
            day = file_datetime.strftime("%d")
            
            # Here, we assume the source is 'kaggle' for this dataset
            partition_path = os.path.join(storage_root, "kaggle", year, month, day)
            os.makedirs(partition_path, exist_ok=True)
            
            # Define the destination path
            destination_path = os.path.join(partition_path, filename)
            
            # Copy the file to the new partitioned location
            shutil.copy(source_file_path, destination_path)
            print(f"Copied {filename} to {destination_path}")

if __name__ == "__main__":
    # Define the source folder where the raw Kaggle dataset was downloaded
    source_folder = os.path.join("data", "raw", "kaggle")
    
    # Define the storage root for partitioned raw data
    storage_root = os.path.join("data", "stored", "raw")
    
    # Run the partitioning process
    partition_raw_data(source_folder, storage_root)