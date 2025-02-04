# src/versioning/data_versioning.py

import os
import subprocess
from datetime import datetime

def run_command(command):
    """
    Run a shell command and print its output.
    
    Args:
        command (str): The shell command to execute.
        
    Returns:
        int: The return code of the command.
    """
    print("Running command:", command)
    result = subprocess.run(command, shell=True, text=True, capture_output=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    return result.returncode

def init_dvc():
    """
    Initialize DVC in the project if it hasn't been initialized yet.
    """
    if not os.path.exists(".dvc"):
        print("Initializing DVC in the project...")
        run_command("dvc init")
    else:
        print("DVC is already initialized.")

def add_data_to_dvc(data_path):
    """
    Add a data directory or file to DVC tracking.
    
    Args:
        data_path (str): The relative path to the data directory or file.
    """
    if os.path.exists(data_path):
        print(f"Adding '{data_path}' to DVC tracking...")
        run_command(f"dvc add {data_path}")
    else:
        print(f"Data path '{data_path}' does not exist.")

def commit_changes(commit_message):
    """
    Commit changes to Git.
    
    Args:
        commit_message (str): The commit message.
    """
    print("Adding changes to Git...")
    run_command("git add .")
    run_command(f'git commit -m "{commit_message}"')

def tag_version(version):
    """
    Tag the current Git commit with a version tag.
    
    Args:
        version (str): The version tag (e.g., v1.0).
    """
    print(f"Tagging the commit with version '{version}'...")
    run_command(f"git tag {version}")

if __name__ == "__main__":
    # Step 1: Initialize DVC.
    init_dvc()
    
    # Step 2: Add raw and processed data to DVC tracking.
    raw_data_path = os.path.join("data", "raw")
    processed_data_path = os.path.join("data", "processed")
    
    add_data_to_dvc(raw_data_path)
    add_data_to_dvc(processed_data_path)
    
    # Step 3: Commit changes with a timestamped message.
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commit_message = f"Data version update: {current_time}"
    commit_changes(commit_message)
    
    # Step 4: Tag the commit with a version identifier.
    tag_version("v1.0")
    
    print("Data versioning complete.")