from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import os

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_netflix_data():
    """
    Hàm tải xuống dataset Netflix từ Kaggle
    """
    import kaggle
    
    # Tạo thư mục data nếu chưa có
    os.makedirs('/tmp/netflix_data', exist_ok=True)
    
    # Download dataset từ Kaggle
    kaggle.api.dataset_download_files(
        'shivamb/netflix-shows', 
        path='/tmp/netflix_data', 
        unzip=True
    )
    
    print("Dataset downloaded successfully to /tmp/netflix_data")

def validate_data():
    """
    Kiểm tra và validate dữ liệu đã tải xuống
    """
    file_path = '/tmp/netflix_data/netflix_titles.csv'
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Dataset file not found at {file_path}")
    
    # Đọc và kiểm tra dữ liệu
    df = pd.read_csv(file_path)
    
    print(f"Dataset shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Missing values per column:")
    print(df.isnull().sum())
    
    # Kiểm tra các cột quan trọng
    required_columns = ['show_id', 'type', 'title', 'release_year']
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    
    print("Data validation completed successfully!")
    return file_path

# Định nghĩa DAG
with DAG(
    'netflix_data_ingestion',
    default_args=default_args,
    description='DAG tải xuống và validate dataset Netflix Shows',
    schedule='@daily',
    catchup=False,
    tags=['netflix', 'data_ingestion', 'etl'],
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    # Task cài đặt dependencies
    install_deps = BashOperator(
        task_id='install_dependencies',
        bash_command='''
        pip install kaggle pandas numpy
        ''',
    )

    # Task tải xuống dữ liệu
    download_data = PythonOperator(
        task_id='download_netflix_data',
        python_callable=download_netflix_data,
    )

    # Task validate dữ liệu
    validate_data_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    # Task backup dữ liệu
    backup_data = BashOperator(
        task_id='backup_data',
        bash_command='''
        mkdir -p /tmp/netflix_backup/$(date +%Y%m%d)
        cp /tmp/netflix_data/netflix_titles.csv /tmp/netflix_backup/$(date +%Y%m%d)/
        echo "Data backed up to /tmp/netflix_backup/$(date +%Y%m%d)/"
        ''',
    )

    end_task = EmptyOperator(
        task_id='end'
    )

    # Định nghĩa dependencies
    start_task >> install_deps >> download_data >> validate_data_task >> backup_data >> end_task
