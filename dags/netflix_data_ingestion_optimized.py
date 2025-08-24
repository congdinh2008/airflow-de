"""
Netflix Data Ingestion Pipeline

This DAG handles the downloading and validation of Netflix Shows dataset from Kaggle.
It includes data quality checks and backup functionality.

Author: Data Engineering Team
Created: 2024-01-01
Last Modified: 2025-08-24
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Constants
DATASET_PATH = "/tmp/netflix_data"
BACKUP_PATH = "/tmp/netflix_backup"
DATASET_FILE = "netflix_titles.csv"
KAGGLE_DATASET = "shivamb/netflix-shows"

# Required columns for validation
REQUIRED_COLUMNS = ["show_id", "type", "title", "release_year", "duration"]

# DAG Configuration
DEFAULT_ARGS = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
}


def download_netflix_dataset() -> str:
    """
    Download Netflix dataset from Kaggle.
    
    Returns:
        str: Path to downloaded dataset file
        
    Raises:
        Exception: If download fails or file is not found
    """
    try:
        import kaggle
        
        # Create data directory if it doesn't exist
        os.makedirs(DATASET_PATH, exist_ok=True)
        
        # Download dataset from Kaggle
        print(f"Downloading dataset: {KAGGLE_DATASET}")
        kaggle.api.dataset_download_files(
            KAGGLE_DATASET, 
            path=DATASET_PATH, 
            unzip=True
        )
        
        dataset_file_path = os.path.join(DATASET_PATH, DATASET_FILE)
        
        if not os.path.exists(dataset_file_path):
            raise FileNotFoundError(f"Dataset file not found at {dataset_file_path}")
            
        print(f"Dataset downloaded successfully to {dataset_file_path}")
        return dataset_file_path
        
    except Exception as e:
        print(f"Error downloading dataset: {str(e)}")
        raise


def validate_dataset() -> dict:
    """
    Validate the downloaded Netflix dataset.
    
    Returns:
        dict: Validation results and dataset metadata
        
    Raises:
        FileNotFoundError: If dataset file is not found
        ValueError: If required columns are missing
        Exception: If validation fails
    """
    dataset_file_path = os.path.join(DATASET_PATH, DATASET_FILE)
    
    try:
        # Check if file exists
        if not os.path.exists(dataset_file_path):
            raise FileNotFoundError(f"Dataset file not found at {dataset_file_path}")
        
        # Read and validate data
        print("Loading dataset for validation...")
        df = pd.read_csv(dataset_file_path)
        
        # Basic dataset info
        print(f"Dataset shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        
        # Check for required columns
        missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Data quality checks
        null_counts = df.isnull().sum()
        print("Missing values per column:")
        print(null_counts.to_string())
        
        # Check for duplicate show_ids
        duplicate_ids = df["show_id"].duplicated().sum()
        print(f"Duplicate show_ids: {duplicate_ids}")
        
        # Validate data types and ranges
        invalid_years = df[
            (df["release_year"] < 1900) | (df["release_year"] > datetime.now().year)
        ].shape[0]
        print(f"Invalid release years: {invalid_years}")
        
        validation_results = {
            "total_records": len(df),
            "total_columns": len(df.columns),
            "missing_values": null_counts.to_dict(),
            "duplicate_show_ids": duplicate_ids,
            "invalid_release_years": invalid_years,
            "validation_date": datetime.now().isoformat(),
            "status": "passed"
        }
        
        print("Data validation completed successfully!")
        return validation_results
        
    except Exception as e:
        print(f"Data validation failed: {str(e)}")
        raise


def create_data_backup() -> str:
    """
    Create backup of the dataset with timestamp.
    
    Returns:
        str: Path to backup directory
    """
    try:
        # Create backup directory with date
        backup_date = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = os.path.join(BACKUP_PATH, backup_date)
        os.makedirs(backup_dir, exist_ok=True)
        
        # Copy dataset to backup location
        source_file = os.path.join(DATASET_PATH, DATASET_FILE)
        backup_file = os.path.join(backup_dir, DATASET_FILE)
        
        # Using pandas to ensure file integrity
        df = pd.read_csv(source_file)
        df.to_csv(backup_file, index=False)
        
        print(f"Data backed up to {backup_dir}")
        return backup_dir
        
    except Exception as e:
        print(f"Backup failed: {str(e)}")
        raise


# DAG Definition
with DAG(
    dag_id="netflix_data_ingestion_optimized",
    default_args=DEFAULT_ARGS,
    description="Netflix Shows dataset ingestion and validation pipeline",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["netflix", "data_ingestion", "etl", "kaggle"],
    doc_md=__doc__,
) as dag:

    # Start task
    start_ingestion = EmptyOperator(
        task_id="start_ingestion",
        doc_md="Pipeline start marker"
    )

    # Install required dependencies
    install_dependencies = BashOperator(
        task_id="install_dependencies",
        bash_command="""
        set -e
        echo "Installing required packages..."
        pip install --quiet kaggle pandas numpy
        echo "Dependencies installed successfully"
        """,
        doc_md="Install required Python packages"
    )

    # Download Netflix dataset
    download_dataset = PythonOperator(
        task_id="download_netflix_dataset",
        python_callable=download_netflix_dataset,
        doc_md="Download Netflix Shows dataset from Kaggle"
    )

    # Validate downloaded data
    validate_dataset_task = PythonOperator(
        task_id="validate_dataset",
        python_callable=validate_dataset,
        doc_md="Validate dataset quality and structure"
    )

    # Create data backup
    backup_dataset = PythonOperator(
        task_id="backup_dataset",
        python_callable=create_data_backup,
        doc_md="Create timestamped backup of dataset"
    )

    # Data quality report
    generate_quality_report = BashOperator(
        task_id="generate_quality_report",
        bash_command=f"""
        set -e
        echo "=== Netflix Dataset Ingestion Report ===" > {DATASET_PATH}/ingestion_report.txt
        echo "Generated on: $(date)" >> {DATASET_PATH}/ingestion_report.txt
        echo "Dataset path: {DATASET_PATH}/{DATASET_FILE}" >> {DATASET_PATH}/ingestion_report.txt
        echo "" >> {DATASET_PATH}/ingestion_report.txt
        
        if [ -f "{DATASET_PATH}/{DATASET_FILE}" ]; then
            echo "File size: $(du -h {DATASET_PATH}/{DATASET_FILE} | cut -f1)" >> {DATASET_PATH}/ingestion_report.txt
            echo "Total records: $(tail -n +2 {DATASET_PATH}/{DATASET_FILE} | wc -l)" >> {DATASET_PATH}/ingestion_report.txt
        fi
        
        echo "Ingestion completed successfully" >> {DATASET_PATH}/ingestion_report.txt
        cat {DATASET_PATH}/ingestion_report.txt
        """,
        doc_md="Generate ingestion quality report"
    )

    # End task
    end_ingestion = EmptyOperator(
        task_id="end_ingestion",
        doc_md="Pipeline completion marker"
    )

    # Define task dependencies
    (
        start_ingestion
        >> install_dependencies
        >> download_dataset
        >> validate_dataset_task
        >> backup_dataset
        >> generate_quality_report
        >> end_ingestion
    )
