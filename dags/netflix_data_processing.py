from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import numpy as np
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

def check_input_file_exists():
    """
    Kiểm tra xem file input đã tồn tại chưa
    """
    import os
    import time
    
    file_path = '/tmp/netflix_data/netflix_titles.csv'
    max_wait = 300  # 5 minutes timeout
    check_interval = 30  # Check every 30 seconds
    
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        if os.path.exists(file_path):
            print(f"Input file found at {file_path}")
            return file_path
        
        print(f"Waiting for input file {file_path}...")
        time.sleep(check_interval)
    
    raise FileNotFoundError(f"Input file {file_path} not found after {max_wait} seconds")

def clean_netflix_data():
    """
    Làm sạch và xử lý dữ liệu Netflix
    """
    # Đọc dữ liệu thô
    input_file = '/tmp/netflix_data/netflix_titles.csv'
    df = pd.read_csv(input_file)
    
    print(f"Original dataset shape: {df.shape}")
    
    # Xử lý missing values
    # Điền các giá trị thiếu cho director
    df['director'] = df['director'].fillna('Unknown Director')
    
    # Điền các giá trị thiếu cho cast
    df['cast'] = df['cast'].fillna('Unknown Cast')
    
    # Điền các giá trị thiếu cho country
    df['country'] = df['country'].fillna('Unknown Country')
    
    # Xử lý date_added
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')
    
    # Loại bỏ các bản ghi có release_year không hợp lệ
    df = df[df['release_year'].notna()]
    df = df[df['release_year'] > 1900]
    df = df[df['release_year'] <= datetime.now().year]
    
    # Tạo cột mới: age_years (tuổi của content)
    current_year = datetime.now().year
    df['age_years'] = current_year - df['release_year']
    
    # Xử lý duration column
    # Tách Movies (phút) và TV Shows (seasons)
    df['duration_minutes'] = np.nan
    df['seasons_count'] = np.nan
    
    # Xử lý Movies - tìm số phút
    movie_mask = df['type'] == 'Movie'
    if movie_mask.sum() > 0:
        # Extract numbers from duration for movies (e.g., "120 min" -> 120)
        duration_values = df.loc[movie_mask, 'duration'].str.extract(r'(\d+)')[0]
        df.loc[movie_mask, 'duration_minutes'] = pd.to_numeric(duration_values, errors='coerce')
    
    # Xử lý TV Shows - tìm số seasons
    tv_mask = df['type'] == 'TV Show'
    if tv_mask.sum() > 0:
        # Extract numbers from duration for TV shows (e.g., "3 Seasons" -> 3)
        season_values = df.loc[tv_mask, 'duration'].str.extract(r'(\d+)')[0]
        df.loc[tv_mask, 'seasons_count'] = pd.to_numeric(season_values, errors='coerce')
    
    print(f"Cleaned dataset shape: {df.shape}")
    
    # Lưu dữ liệu đã làm sạch
    output_file = '/tmp/netflix_data/netflix_titles_cleaned.csv'
    df.to_csv(output_file, index=False)
    
    print(f"Cleaned data saved to {output_file}")
    return output_file

def create_derived_features():
    """
    Tạo các features mới từ dữ liệu
    """
    input_file = '/tmp/netflix_data/netflix_titles_cleaned.csv'
    df = pd.read_csv(input_file)
    
    # Tạo features mới
    # Đếm số lượng countries cho mỗi title
    df['country_count'] = df['country'].apply(lambda x: len(str(x).split(',')) if pd.notna(x) else 0)
    
    # Đếm số lượng genres cho mỗi title
    df['genre_count'] = df['listed_in'].apply(lambda x: len(str(x).split(',')) if pd.notna(x) else 0)
    
    # Tạo decade column
    df['decade'] = (df['release_year'] // 10) * 10
    
    # Tạo content_category dựa trên rating
    rating_categories = {
        'G': 'Family',
        'PG': 'Family',
        'PG-13': 'Teen',
        'R': 'Adult',
        'NC-17': 'Adult',
        'TV-Y': 'Kids',
        'TV-Y7': 'Kids',
        'TV-G': 'Family',
        'TV-PG': 'Family',
        'TV-14': 'Teen',
        'TV-MA': 'Adult'
    }
    df['content_category'] = df['rating'].map(rating_categories).fillna('Other')
    
    # Lưu dữ liệu với features mới
    output_file = '/tmp/netflix_data/netflix_titles_features.csv'
    df.to_csv(output_file, index=False)
    
    print(f"Enhanced dataset with new features saved to {output_file}")
    return output_file

def split_data_by_type():
    """
    Tách dữ liệu thành Movies và TV Shows riêng biệt
    """
    input_file = '/tmp/netflix_data/netflix_titles_features.csv'
    df = pd.read_csv(input_file)
    
    # Tách Movies
    movies_df = df[df['type'] == 'Movie'].copy()
    movies_file = '/tmp/netflix_data/netflix_movies.csv'
    movies_df.to_csv(movies_file, index=False)
    
    # Tách TV Shows
    tv_shows_df = df[df['type'] == 'TV Show'].copy()
    tv_shows_file = '/tmp/netflix_data/netflix_tv_shows.csv'
    tv_shows_df.to_csv(tv_shows_file, index=False)
    
    print(f"Movies dataset: {movies_df.shape[0]} records saved to {movies_file}")
    print(f"TV Shows dataset: {tv_shows_df.shape[0]} records saved to {tv_shows_file}")

# Định nghĩa DAG
with DAG(
    'netflix_data_processing',
    default_args=default_args,
    description='DAG xử lý và làm sạch dữ liệu Netflix Shows',
    schedule='@daily',
    catchup=False,
    tags=['netflix', 'data_processing', 'etl'],
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    # Task kiểm tra file input đã có chưa
    check_input_file = PythonOperator(
        task_id='check_input_file',
        python_callable=check_input_file_exists,
    )

    # Task làm sạch dữ liệu
    clean_data = PythonOperator(
        task_id='clean_netflix_data',
        python_callable=clean_netflix_data,
    )

    # Task tạo features mới
    create_features = PythonOperator(
        task_id='create_derived_features',
        python_callable=create_derived_features,
    )

    # Task tách dữ liệu theo loại
    split_data = PythonOperator(
        task_id='split_data_by_type',
        python_callable=split_data_by_type,
    )

    # Task tạo data quality report
    quality_check = BashOperator(
        task_id='data_quality_check',
        bash_command='''
        echo "=== Data Quality Report ===" > /tmp/netflix_data/quality_report.txt
        echo "Generated on: $(date)" >> /tmp/netflix_data/quality_report.txt
        echo "" >> /tmp/netflix_data/quality_report.txt
        
        if [ -f "/tmp/netflix_data/netflix_movies.csv" ]; then
            movies_count=$(tail -n +2 /tmp/netflix_data/netflix_movies.csv | wc -l)
            echo "Movies count: $movies_count" >> /tmp/netflix_data/quality_report.txt
        fi
        
        if [ -f "/tmp/netflix_data/netflix_tv_shows.csv" ]; then
            tv_count=$(tail -n +2 /tmp/netflix_data/netflix_tv_shows.csv | wc -l)
            echo "TV Shows count: $tv_count" >> /tmp/netflix_data/quality_report.txt
        fi
        
        echo "Quality check completed!"
        cat /tmp/netflix_data/quality_report.txt
        ''',
    )

    end_task = EmptyOperator(
        task_id='end'
    )

    # Định nghĩa dependencies
    start_task >> check_input_file >> clean_data >> create_features >> split_data >> quality_check >> end_task
