## workspace/clearcare_compliance/app/utils.py

import os
import zipfile
from typing import Dict, Any
from fastapi import UploadFile

def save_uploaded_file(upload_file: UploadFile, destination: str) -> str:
    """Saves the uploaded file to the specified destination.

    Args:
        upload_file (UploadFile): The file to be saved.
        destination (str): The path where the file will be saved.

    Returns:
        str: The path of the saved file.
    """
    with open(destination, "wb") as buffer:
        buffer.write(upload_file.file.read())
    return destination

def create_zip_file(zip_name: str, files: Dict[str, str]) -> str:
    """Creates a zip file containing the specified files.

    Args:
        zip_name (str): The name of the zip file to be created.
        files (Dict[str, str]): A dictionary where keys are file names and values are file paths.

    Returns:
        str: The path of the created zip file.
    """
    with zipfile.ZipFile(zip_name, 'w') as zip_file:
        for file_name, file_path in files.items():
            zip_file.write(file_path, arcname=file_name)
    return zip_name

def generate_evidence_pack_files(run_id: str, html_content: str, json_data: Dict[str, Any], csv_data: str) -> Dict[str, str]:
    """Generates the files needed for an evidence pack.

    Args:
        run_id (str): The identifier for the compliance run.
        html_content (str): The HTML content of the evidence pack.
        json_data (Dict[str, Any]): The JSON data of the evidence pack.
        csv_data (str): The CSV data of the evidence pack.

    Returns:
        Dict[str, str]: A dictionary containing paths to the generated files.
    """
    base_path = f"evidence_packs/{run_id}"
    os.makedirs(base_path, exist_ok=True)

    html_file_path = os.path.join(base_path, f"{run_id}.html")
    json_file_path = os.path.join(base_path, f"{run_id}.json")
    csv_file_path = os.path.join(base_path, f"{run_id}.csv")

    with open(html_file_path, "w") as html_file:
        html_file.write(html_content)

    with open(json_file_path, "w") as json_file:
        json_file.write(json.dumps(json_data))

    with open(csv_file_path, "w") as csv_file:
        csv_file.write(csv_data)

    return {
        "html": html_file_path,
        "json": json_file_path,
        "csv": csv_file_path
    }
