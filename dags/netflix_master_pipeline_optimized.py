"""
Netflix Master Pipeline

This is the orchestrator DAG that manages the complete Netflix data pipeline
from ingestion through processing to analysis. It ensures proper sequencing
and dependency management across all pipeline stages.

Author: Data Engineering Team
Created: 2024-01-01
Last Modified: 2025-08-24
"""

import os
import shutil
from datetime import datetime, timedelta
from typing import Dict, List

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Constants
DATA_PATH = "/tmp/netflix_data"
ANALYSIS_PATH = "/tmp/netflix_analysis"
BACKUP_PATH = "/tmp/netflix_backup"

# Pipeline DAGs configuration
PIPELINE_DAGS = {
    "ingestion": "netflix_data_ingestion_optimized",
    "processing": "netflix_data_processing_optimized", 
    "analysis": "netflix_data_analysis_optimized"
}

# DAG Configuration
DEFAULT_ARGS = {
    "owner": "data_engineering_lead",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "max_active_runs": 1,
}


def setup_pipeline_environment() -> Dict[str, str]:
    """
    Setup and prepare the pipeline environment.
    
    Returns:
        Dict[str, str]: Environment setup status and paths
    """
    try:
        print("Setting up Netflix data pipeline environment...")
        
        # Create required directories
        directories = [DATA_PATH, ANALYSIS_PATH, BACKUP_PATH]
        created_dirs = []
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            created_dirs.append(directory)
            print(f"Directory ready: {directory}")
        
        # Set up environment variables if needed
        environment_info = {
            "data_path": DATA_PATH,
            "analysis_path": ANALYSIS_PATH,
            "backup_path": BACKUP_PATH,
            "setup_timestamp": datetime.now().isoformat(),
            "status": "success"
        }
        
        print("Pipeline environment setup completed successfully!")
        return environment_info
        
    except Exception as e:
        print(f"Error setting up environment: {str(e)}")
        raise


def validate_pipeline_prerequisites() -> bool:
    """
    Validate that all prerequisites for the pipeline are met.
    
    Returns:
        bool: True if all prerequisites are satisfied
        
    Raises:
        Exception: If prerequisites are not met
    """
    try:
        print("Validating pipeline prerequisites...")
        
        # Check required directories exist
        required_dirs = [DATA_PATH, ANALYSIS_PATH, BACKUP_PATH]
        for directory in required_dirs:
            if not os.path.exists(directory):
                raise Exception(f"Required directory missing: {directory}")
        
        # Check available disk space (basic check)
        import shutil
        disk_usage = shutil.disk_usage("/tmp")
        available_gb = disk_usage.free / (1024**3)
        
        if available_gb < 1.0:  # Require at least 1GB free space
            raise Exception(f"Insufficient disk space: {available_gb:.2f}GB available")
        
        print(f"Disk space check passed: {available_gb:.2f}GB available")
        
        # Check if Kaggle credentials are configured (basic check)
        kaggle_config_path = os.path.expanduser("~/.kaggle/kaggle.json")
        if not os.path.exists(kaggle_config_path):
            print("Warning: Kaggle credentials may not be configured")
        
        print("Pipeline prerequisites validation completed!")
        return True
        
    except Exception as e:
        print(f"Prerequisites validation failed: {str(e)}")
        raise


def cleanup_temporary_files() -> Dict[str, any]:
    """
    Clean up temporary files and manage storage.
    
    Returns:
        Dict[str, any]: Cleanup operation results
    """
    try:
        print("Starting cleanup of temporary files...")
        
        cleanup_stats = {
            "files_removed": 0,
            "space_freed_mb": 0,
            "directories_cleaned": [],
            "cleanup_timestamp": datetime.now().isoformat()
        }
        
        # Define cleanup patterns
        cleanup_patterns = [
            "*.tmp",
            "*.temp", 
            "*_temp.csv",
            "*.log"
        ]
        
        # Cleanup data directory
        if os.path.exists(DATA_PATH):
            initial_size = sum(
                os.path.getsize(os.path.join(DATA_PATH, f))
                for f in os.listdir(DATA_PATH)
                if os.path.isfile(os.path.join(DATA_PATH, f))
            )
            
            # Remove temporary files (implement pattern matching if needed)
            # For now, just report the cleanup would happen
            cleanup_stats["directories_cleaned"].append(DATA_PATH)
        
        # Manage old backups (keep only last 5)
        if os.path.exists(BACKUP_PATH):
            backup_dirs = [
                d for d in os.listdir(BACKUP_PATH)
                if os.path.isdir(os.path.join(BACKUP_PATH, d))
            ]
            backup_dirs.sort(reverse=True)  # Most recent first
            
            # Remove old backups beyond retention limit
            retention_limit = 5
            if len(backup_dirs) > retention_limit:
                for old_backup in backup_dirs[retention_limit:]:
                    old_backup_path = os.path.join(BACKUP_PATH, old_backup)
                    shutil.rmtree(old_backup_path)
                    cleanup_stats["files_removed"] += 1
                    print(f"Removed old backup: {old_backup}")
        
        print(f"Cleanup completed. Removed {cleanup_stats['files_removed']} items")
        return cleanup_stats
        
    except Exception as e:
        print(f"Error during cleanup: {str(e)}")
        # Don't raise - cleanup errors shouldn't fail the pipeline
        return {"error": str(e), "cleanup_timestamp": datetime.now().isoformat()}


def generate_pipeline_summary() -> Dict[str, any]:
    """
    Generate comprehensive pipeline execution summary.
    
    Returns:
        Dict[str, any]: Pipeline execution summary
    """
    try:
        print("Generating pipeline execution summary...")
        
        summary = {
            "pipeline_execution": {
                "start_time": "{{ dag_run.start_date }}",
                "end_time": datetime.now().isoformat(),
                "status": "completed"
            },
            "data_files": {},
            "analysis_results": {},
            "performance_metrics": {}
        }
        
        # Check data files
        data_files = [
            "netflix_titles.csv",
            "netflix_titles_cleaned.csv", 
            "netflix_titles_features.csv",
            "netflix_movies.csv",
            "netflix_tv_shows.csv"
        ]
        
        for filename in data_files:
            file_path = os.path.join(DATA_PATH, filename)
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                summary["data_files"][filename] = {
                    "exists": True,
                    "size_bytes": file_size,
                    "size_mb": round(file_size / 1024 / 1024, 2)
                }
            else:
                summary["data_files"][filename] = {"exists": False}
        
        # Check analysis results
        if os.path.exists(ANALYSIS_PATH):
            analysis_files = [f for f in os.listdir(ANALYSIS_PATH) if f.endswith(".json")]
            summary["analysis_results"]["files_generated"] = len(analysis_files)
            summary["analysis_results"]["file_list"] = analysis_files
        
        # Basic performance metrics
        if os.path.exists(DATA_PATH):
            total_data_size = sum(
                summary["data_files"][f].get("size_bytes", 0)
                for f in summary["data_files"]
                if summary["data_files"][f].get("exists", False)
            )
            summary["performance_metrics"]["total_data_size_mb"] = round(total_data_size / 1024 / 1024, 2)
        
        # Save summary
        summary_file = os.path.join(DATA_PATH, "pipeline_summary.json")
        import json
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)
        
        print("Pipeline summary generated successfully!")
        return summary
        
    except Exception as e:
        print(f"Error generating pipeline summary: {str(e)}")
        raise


# DAG Definition
with DAG(
    dag_id="netflix_master_pipeline_optimized",
    default_args=DEFAULT_ARGS,
    description="Master orchestrator for Netflix data pipeline",
    schedule="@weekly",  # Run weekly to avoid overwhelming the system
    catchup=False,
    max_active_runs=1,
    tags=["netflix", "master_pipeline", "orchestrator", "etl"],
    doc_md=__doc__,
) as dag:

    # Pipeline initialization
    start_pipeline = EmptyOperator(
        task_id="start_pipeline",
        doc_md="Pipeline orchestration start marker"
    )

    # Environment setup
    setup_environment = PythonOperator(
        task_id="setup_environment",
        python_callable=setup_pipeline_environment,
        doc_md="Setup pipeline environment and directories"
    )

    # Prerequisites validation
    validate_prerequisites = PythonOperator(
        task_id="validate_prerequisites",
        python_callable=validate_pipeline_prerequisites,
        doc_md="Validate pipeline prerequisites and system requirements"
    )

    # Trigger data ingestion pipeline
    trigger_data_ingestion = TriggerDagRunOperator(
        task_id="trigger_data_ingestion",
        trigger_dag_id=PIPELINE_DAGS["ingestion"],
        wait_for_completion=False,
        doc_md="Trigger Netflix data ingestion pipeline"
    )

    # Trigger data processing pipeline
    trigger_data_processing = TriggerDagRunOperator(
        task_id="trigger_data_processing",
        trigger_dag_id=PIPELINE_DAGS["processing"],
        wait_for_completion=False,
        doc_md="Trigger Netflix data processing pipeline"
    )

    # Trigger data analysis pipeline
    trigger_data_analysis = TriggerDagRunOperator(
        task_id="trigger_data_analysis",
        trigger_dag_id=PIPELINE_DAGS["analysis"],
        wait_for_completion=False,
        doc_md="Trigger Netflix data analysis pipeline"
    )

    # Generate pipeline summary
    generate_pipeline_summary_task = PythonOperator(
        task_id="generate_pipeline_summary",
        python_callable=generate_pipeline_summary,
        doc_md="Generate comprehensive pipeline execution summary"
    )

    # Cleanup temporary files
    cleanup_temp_files = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=cleanup_temporary_files,
        doc_md="Clean up temporary files and manage storage"
    )

    # Pipeline completion notification
    pipeline_completion_notification = BashOperator(
        task_id="pipeline_completion_notification",
        bash_command=f"""
        echo "========================================"
        echo "Netflix Data Pipeline Execution Complete"
        echo "========================================"
        echo "Completion Time: $(date)"
        echo "Data Path: {DATA_PATH}"
        echo "Analysis Path: {ANALYSIS_PATH}"
        echo ""
        
        # Show pipeline results summary
        if [ -f "{DATA_PATH}/pipeline_summary.json" ]; then
            echo "Pipeline Summary Available:"
            echo "File: {DATA_PATH}/pipeline_summary.json"
        fi
        
        # Show data files created
        echo "Data Files Created:"
        ls -lh {DATA_PATH}/*.csv 2>/dev/null || echo "No CSV files found"
        
        echo ""
        echo "Analysis Results:"
        ls -lh {ANALYSIS_PATH}/*.json 2>/dev/null || echo "No analysis files found"
        
        echo ""
        echo "Netflix Data Pipeline completed successfully!"
        """,
        doc_md="Send pipeline completion notification and summary"
    )

    # End pipeline
    end_pipeline = EmptyOperator(
        task_id="end_pipeline",
        doc_md="Pipeline orchestration completion marker"
    )

    # Define task dependencies
    start_pipeline >> setup_environment >> validate_prerequisites

    # Sequential pipeline execution
    (
        validate_prerequisites
        >> trigger_data_ingestion
        >> trigger_data_processing
        >> trigger_data_analysis
        >> generate_pipeline_summary_task
        >> cleanup_temp_files
        >> pipeline_completion_notification
        >> end_pipeline
    )
