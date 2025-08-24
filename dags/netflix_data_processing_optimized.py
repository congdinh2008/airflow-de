"""
Netflix Data Processing Pipeline

This DAG handles data cleaning, transformation, and feature engineering
for the Netflix Shows dataset. It prepares the data for analysis.

Author: Data Engineering Team
Created: 2024-01-01
Last Modified: 2025-08-24
"""

import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Constants
DATA_PATH = "/tmp/netflix_data"
INPUT_FILE = "netflix_titles.csv"
CLEANED_FILE = "netflix_titles_cleaned.csv"
FEATURES_FILE = "netflix_titles_features.csv"
MOVIES_FILE = "netflix_movies.csv"
TV_SHOWS_FILE = "netflix_tv_shows.csv"

# Configuration
WAIT_TIMEOUT = 300  # 5 minutes
CHECK_INTERVAL = 30  # 30 seconds

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

# Content rating categories mapping
RATING_CATEGORIES = {
    "G": "Family",
    "PG": "Family", 
    "PG-13": "Teen",
    "R": "Adult",
    "NC-17": "Adult",
    "TV-Y": "Kids",
    "TV-Y7": "Kids",
    "TV-G": "Family",
    "TV-PG": "Family",
    "TV-14": "Teen",
    "TV-MA": "Adult"
}


def wait_for_input_file() -> str:
    """
    Wait for input file to become available.
    
    Returns:
        str: Path to the input file
        
    Raises:
        FileNotFoundError: If file is not found within timeout
    """
    file_path = os.path.join(DATA_PATH, INPUT_FILE)
    start_time = time.time()
    
    print(f"Waiting for input file: {file_path}")
    
    while time.time() - start_time < WAIT_TIMEOUT:
        if os.path.exists(file_path):
            print(f"Input file found: {file_path}")
            return file_path
        
        print(f"File not found, waiting... ({int(time.time() - start_time)}s elapsed)")
        time.sleep(CHECK_INTERVAL)
    
    raise FileNotFoundError(
        f"Input file {file_path} not found after {WAIT_TIMEOUT} seconds"
    )


def clean_dataset() -> str:
    """
    Clean and preprocess the Netflix dataset.
    
    Returns:
        str: Path to cleaned dataset file
        
    Raises:
        Exception: If cleaning process fails
    """
    try:
        # Load raw data
        input_path = os.path.join(DATA_PATH, INPUT_FILE)
        print(f"Loading dataset from: {input_path}")
        df = pd.read_csv(input_path)
        
        print(f"Original dataset shape: {df.shape}")
        
        # Data cleaning operations
        print("Starting data cleaning...")
        
        # 1. Remove exact duplicates
        initial_count = len(df)
        df = df.drop_duplicates()
        print(f"Removed {initial_count - len(df)} duplicate rows")
        
        # 2. Clean text fields
        text_columns = ["title", "director", "cast", "country", "description"]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace("nan", pd.NA)
        
        # 3. Clean and standardize date_added
        if "date_added" in df.columns:
            df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce")
        
        # 4. Validate and clean release_year
        current_year = datetime.now().year
        df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
        df = df[
            (df["release_year"] >= 1900) & 
            (df["release_year"] <= current_year)
        ]
        
        # 5. Calculate age of content
        df["age_years"] = current_year - df["release_year"]
        
        # 6. Parse duration information
        df = _parse_duration_data(df)
        
        # Save cleaned data
        output_path = os.path.join(DATA_PATH, CLEANED_FILE)
        df.to_csv(output_path, index=False)
        
        print(f"Cleaned dataset shape: {df.shape}")
        print(f"Cleaned data saved to: {output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"Error during data cleaning: {str(e)}")
        raise


def _parse_duration_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Parse duration information for movies and TV shows.
    
    Args:
        df: Input DataFrame
        
    Returns:
        pd.DataFrame: DataFrame with parsed duration columns
    """
    print("Parsing duration information...")
    
    # Initialize duration columns
    df["duration_minutes"] = pd.NA
    df["seasons_count"] = pd.NA
    
    # Parse movie durations (e.g., "90 min")
    movie_mask = df["type"] == "Movie"
    if movie_mask.any():
        movie_durations = df.loc[movie_mask, "duration"].str.extract(r"(\d+)")[0]
        df.loc[movie_mask, "duration_minutes"] = pd.to_numeric(
            movie_durations, errors="coerce"
        )
    
    # Parse TV show seasons (e.g., "3 Seasons")
    tv_mask = df["type"] == "TV Show"
    if tv_mask.any():
        tv_seasons = df.loc[tv_mask, "duration"].str.extract(r"(\d+)")[0]
        df.loc[tv_mask, "seasons_count"] = pd.to_numeric(
            tv_seasons, errors="coerce"
        )
    
    return df


def create_enhanced_features() -> str:
    """
    Create derived features from cleaned data.
    
    Returns:
        str: Path to enhanced dataset file
        
    Raises:
        Exception: If feature creation fails
    """
    try:
        # Load cleaned data
        input_path = os.path.join(DATA_PATH, CLEANED_FILE)
        print(f"Loading cleaned data from: {input_path}")
        df = pd.read_csv(input_path)
        
        print("Creating enhanced features...")
        
        # 1. Country-related features
        df["country_count"] = df["country"].apply(
            lambda x: len(str(x).split(",")) if pd.notna(x) else 0
        )
        
        # 2. Genre-related features
        df["genre_count"] = df["listed_in"].apply(
            lambda x: len(str(x).split(",")) if pd.notna(x) else 0
        )
        
        # 3. Time-based features
        df["decade"] = (df["release_year"] // 10) * 10
        
        # 4. Content categorization
        df["content_category"] = df["rating"].map(RATING_CATEGORIES).fillna("Other")
        
        # 5. Content freshness indicators
        df["is_recent"] = df["age_years"] <= 5
        df["is_classic"] = df["age_years"] >= 20
        
        # 6. Duration-based features for movies
        if "duration_minutes" in df.columns:
            df["duration_category"] = pd.cut(
                df["duration_minutes"],
                bins=[0, 90, 120, 180, float("inf")],
                labels=["Short", "Medium", "Long", "Very Long"],
                include_lowest=True
            )
        
        # Save enhanced dataset
        output_path = os.path.join(DATA_PATH, FEATURES_FILE)
        df.to_csv(output_path, index=False)
        
        print(f"Enhanced dataset with {len(df.columns)} features saved to: {output_path}")
        return output_path
        
    except Exception as e:
        print(f"Error creating features: {str(e)}")
        raise


def split_by_content_type() -> Tuple[str, str]:
    """
    Split dataset into separate files for movies and TV shows.
    
    Returns:
        Tuple[str, str]: Paths to movies and TV shows files
        
    Raises:
        Exception: If splitting fails
    """
    try:
        # Load enhanced dataset
        input_path = os.path.join(DATA_PATH, FEATURES_FILE)
        print(f"Loading enhanced data from: {input_path}")
        df = pd.read_csv(input_path)
        
        # Split by content type
        movies_df = df[df["type"] == "Movie"].copy()
        tv_shows_df = df[df["type"] == "TV Show"].copy()
        
        # Save movies dataset
        movies_path = os.path.join(DATA_PATH, MOVIES_FILE)
        movies_df.to_csv(movies_path, index=False)
        print(f"Movies dataset: {len(movies_df)} records saved to {movies_path}")
        
        # Save TV shows dataset
        tv_shows_path = os.path.join(DATA_PATH, TV_SHOWS_FILE)
        tv_shows_df.to_csv(tv_shows_path, index=False)
        print(f"TV Shows dataset: {len(tv_shows_df)} records saved to {tv_shows_path}")
        
        return movies_path, tv_shows_path
        
    except Exception as e:
        print(f"Error splitting data: {str(e)}")
        raise


def generate_processing_report() -> Dict[str, any]:
    """
    Generate comprehensive data processing report.
    
    Returns:
        Dict: Processing statistics and metadata
    """
    try:
        report = {
            "processing_date": datetime.now().isoformat(),
            "files_created": [],
            "statistics": {}
        }
        
        # Check all created files
        files_to_check = [
            (CLEANED_FILE, "cleaned_data"),
            (FEATURES_FILE, "enhanced_features"),
            (MOVIES_FILE, "movies_split"),
            (TV_SHOWS_FILE, "tv_shows_split")
        ]
        
        for filename, file_type in files_to_check:
            file_path = os.path.join(DATA_PATH, filename)
            if os.path.exists(file_path):
                df = pd.read_csv(file_path)
                report["files_created"].append({
                    "file": filename,
                    "type": file_type,
                    "records": len(df),
                    "columns": len(df.columns),
                    "size_mb": round(os.path.getsize(file_path) / 1024 / 1024, 2)
                })
        
        return report
        
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        return {"error": str(e)}


# DAG Definition
with DAG(
    dag_id="netflix_data_processing_optimized",
    default_args=DEFAULT_ARGS,
    description="Netflix Shows data cleaning and feature engineering pipeline",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["netflix", "data_processing", "etl", "feature_engineering"],
    doc_md=__doc__,
) as dag:

    # Start task
    start_processing = EmptyOperator(
        task_id="start_processing",
        doc_md="Processing pipeline start marker"
    )

    # Wait for input file
    check_input_file = PythonOperator(
        task_id="check_input_file",
        python_callable=wait_for_input_file,
        doc_md="Wait for input dataset file to be available"
    )

    # Clean raw data
    clean_netflix_data = PythonOperator(
        task_id="clean_netflix_data",
        python_callable=clean_dataset,
        doc_md="Clean and preprocess raw Netflix dataset"
    )

    # Create enhanced features
    create_derived_features = PythonOperator(
        task_id="create_derived_features",
        python_callable=create_enhanced_features,
        doc_md="Create derived features for analysis"
    )

    # Split by content type
    split_data_by_type = PythonOperator(
        task_id="split_data_by_type",
        python_callable=split_by_content_type,
        doc_md="Split dataset into movies and TV shows"
    )

    # Data quality assessment
    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command=f"""
        set -e
        echo "=== Data Processing Quality Report ===" > {DATA_PATH}/processing_report.txt
        echo "Generated on: $(date)" >> {DATA_PATH}/processing_report.txt
        echo "" >> {DATA_PATH}/processing_report.txt
        
        for file in {CLEANED_FILE} {FEATURES_FILE} {MOVIES_FILE} {TV_SHOWS_FILE}; do
            if [ -f "{DATA_PATH}/$file" ]; then
                echo "File: $file" >> {DATA_PATH}/processing_report.txt
                echo "  Records: $(tail -n +2 {DATA_PATH}/$file | wc -l)" >> {DATA_PATH}/processing_report.txt
                echo "  Size: $(du -h {DATA_PATH}/$file | cut -f1)" >> {DATA_PATH}/processing_report.txt
                echo "" >> {DATA_PATH}/processing_report.txt
            fi
        done
        
        echo "Processing completed successfully" >> {DATA_PATH}/processing_report.txt
        cat {DATA_PATH}/processing_report.txt
        """,
        doc_md="Generate data quality and processing report"
    )

    # End task
    end_processing = EmptyOperator(
        task_id="end_processing",
        doc_md="Processing pipeline completion marker"
    )

    # Define task dependencies
    (
        start_processing
        >> check_input_file
        >> clean_netflix_data
        >> create_derived_features
        >> split_data_by_type
        >> data_quality_check
        >> end_processing
    )
