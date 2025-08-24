from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pandas as pd
import numpy as np
import json
import os

default_args = {
    'owner': 'data_analyst',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_processed_data_exists():
    """
    Kiểm tra xem file processed data đã tồn tại chưa
    """
    import os
    import time
    
    file_path = '/tmp/netflix_data/netflix_titles_features.csv'
    max_wait = 300  # 5 minutes timeout
    check_interval = 30  # Check every 30 seconds
    
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        if os.path.exists(file_path):
            print(f"Processed data file found at {file_path}")
            return file_path
        
        print(f"Waiting for processed data file {file_path}...")
        time.sleep(check_interval)
    
    raise FileNotFoundError(f"Processed data file {file_path} not found after {max_wait} seconds")

def analyze_content_trends():
    """
    Phân tích xu hướng nội dung theo thời gian
    """
    df = pd.read_csv('/tmp/netflix_data/netflix_titles_features.csv')
    
    # Phân tích theo năm phát hành
    yearly_stats = df.groupby(['release_year', 'type']).size().unstack(fill_value=0)
    
    # Phân tích theo thập kỷ
    decade_stats = df.groupby(['decade', 'type']).size().unstack(fill_value=0)
    
    # Top 10 năm có nhiều content nhất
    content_by_year = df['release_year'].value_counts().head(10)
    
    # Lưu kết quả
    os.makedirs('/tmp/netflix_analysis', exist_ok=True)
    
    results = {
        'yearly_stats': yearly_stats.to_dict(),
        'decade_stats': decade_stats.to_dict(),
        'top_years': content_by_year.to_dict(),
        'analysis_date': datetime.now().isoformat()
    }
    
    with open('/tmp/netflix_analysis/content_trends.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("Content trends analysis completed!")
    return results

def analyze_ratings_distribution():
    """
    Phân tích phân phối ratings và content categories
    """
    df = pd.read_csv('/tmp/netflix_data/netflix_titles_features.csv')
    
    # Phân tích rating distribution
    rating_dist = df['rating'].value_counts()
    
    # Phân tích content category distribution
    category_dist = df['content_category'].value_counts()
    
    # Phân tích rating theo type
    rating_by_type = df.groupby(['type', 'rating']).size().unstack(fill_value=0)
    
    # Movies vs TV Shows ratio
    type_ratio = df['type'].value_counts(normalize=True)
    
    results = {
        'rating_distribution': rating_dist.to_dict(),
        'category_distribution': category_dist.to_dict(),
        'rating_by_type': rating_by_type.to_dict(),
        'type_ratio': type_ratio.to_dict(),
        'analysis_date': datetime.now().isoformat()
    }
    
    with open('/tmp/netflix_analysis/ratings_analysis.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("Ratings distribution analysis completed!")
    return results

def analyze_countries_and_genres():
    """
    Phân tích các quốc gia và thể loại phổ biến
    """
    df = pd.read_csv('/tmp/netflix_data/netflix_titles_features.csv')
    
    # Top countries
    all_countries = []
    for countries in df['country'].dropna():
        all_countries.extend([c.strip() for c in str(countries).split(',')])
    
    country_counts = pd.Series(all_countries).value_counts().head(20)
    
    # Top genres
    all_genres = []
    for genres in df['listed_in'].dropna():
        all_genres.extend([g.strip() for g in str(genres).split(',')])
    
    genre_counts = pd.Series(all_genres).value_counts().head(20)
    
    # Average content age by country (top 10 countries)
    top_countries = country_counts.head(10).index
    country_age_stats = {}
    
    for country in top_countries:
        country_content = df[df['country'].str.contains(country, na=False)]
        avg_age = country_content['age_years'].mean()
        country_age_stats[country] = round(avg_age, 2)
    
    results = {
        'top_countries': country_counts.to_dict(),
        'top_genres': genre_counts.to_dict(),
        'country_age_stats': country_age_stats,
        'analysis_date': datetime.now().isoformat()
    }
    
    with open('/tmp/netflix_analysis/countries_genres_analysis.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("Countries and genres analysis completed!")
    return results

def analyze_duration_patterns():
    """
    Phân tích patterns về duration của Movies và TV Shows
    """
    df = pd.read_csv('/tmp/netflix_data/netflix_titles_features.csv')
    
    # Movies duration analysis
    movies_df = df[df['type'] == 'Movie'].copy()
    
    # Check if we have valid duration data for movies
    valid_movie_durations = movies_df['duration_minutes'].dropna()
    
    if len(valid_movie_durations) > 0:
        movie_duration_stats = {
            'avg_duration': float(valid_movie_durations.mean()),
            'median_duration': float(valid_movie_durations.median()),
            'min_duration': float(valid_movie_durations.min()),
            'max_duration': float(valid_movie_durations.max()),
            'std_duration': float(valid_movie_durations.std())
        }
    else:
        movie_duration_stats = {
            'avg_duration': 0.0,
            'median_duration': 0.0,
            'min_duration': 0.0,
            'max_duration': 0.0,
            'std_duration': 0.0
        }
    
    # TV Shows seasons analysis
    tv_df = df[df['type'] == 'TV Show'].copy()
    
    # Check if we have valid seasons data for TV shows
    valid_tv_seasons = tv_df['seasons_count'].dropna()
    
    if len(valid_tv_seasons) > 0:
        tv_seasons_stats = {
            'avg_seasons': float(valid_tv_seasons.mean()),
            'median_seasons': float(valid_tv_seasons.median()),
            'min_seasons': float(valid_tv_seasons.min()),
            'max_seasons': float(valid_tv_seasons.max()),
            'single_season_shows': int((valid_tv_seasons == 1).sum())
        }
    else:
        tv_seasons_stats = {
            'avg_seasons': 0.0,
            'median_seasons': 0.0,
            'min_seasons': 0.0,
            'max_seasons': 0.0,
            'single_season_shows': 0
        }
    
    # Duration trends over time for movies (only if we have data)
    if len(valid_movie_durations) > 0:
        movies_with_duration = movies_df.dropna(subset=['duration_minutes'])
        if len(movies_with_duration) > 0:
            movies_by_decade = movies_with_duration.groupby('decade')['duration_minutes'].agg(['mean', 'count'])
        else:
            movies_by_decade = pd.DataFrame(columns=['mean', 'count'])
    else:
        movies_by_decade = pd.DataFrame(columns=['mean', 'count'])
    
    results = {
        'movie_duration_stats': movie_duration_stats,
        'tv_seasons_stats': tv_seasons_stats,
        'movies_duration_by_decade': movies_by_decade.to_dict() if not movies_by_decade.empty else {'mean': {}, 'count': {}},
        'analysis_date': datetime.now().isoformat(),
        'data_quality': {
            'movies_with_duration_data': len(valid_movie_durations),
            'tv_shows_with_seasons_data': len(valid_tv_seasons),
            'total_movies': len(movies_df),
            'total_tv_shows': len(tv_df)
        }
    }
    
    with open('/tmp/netflix_analysis/duration_patterns.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("Duration patterns analysis completed!")
    print(f"Movies with duration data: {len(valid_movie_durations)}/{len(movies_df)}")
    print(f"TV shows with seasons data: {len(valid_tv_seasons)}/{len(tv_df)}")
    return results

def generate_summary_report():
    """
    Tạo báo cáo tổng hợp từ tất cả các phân tích
    """
    # Đọc tất cả các file phân tích
    analysis_files = [
        '/tmp/netflix_analysis/content_trends.json',
        '/tmp/netflix_analysis/ratings_analysis.json', 
        '/tmp/netflix_analysis/countries_genres_analysis.json',
        '/tmp/netflix_analysis/duration_patterns.json'
    ]
    
    summary_data = {}
    for file_path in analysis_files:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                data = json.load(f)
                filename = os.path.basename(file_path).replace('.json', '')
                summary_data[filename] = data
    
    # Tạo summary report
    report_content = f"""
# Netflix Dataset Analysis Summary Report
Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Key Insights:

### Content Volume:
- Total analysis files processed: {len(summary_data)}

### Data Quality:
- Analysis completed successfully for all major dimensions
- Files generated: {', '.join(summary_data.keys())}

## Detailed Analysis:
Each analysis dimension has been saved to separate JSON files for further processing.

### Available Analysis Files:
"""
    
    for key in summary_data.keys():
        report_content += f"- {key}.json\n"
    
    # Lưu summary report
    with open('/tmp/netflix_analysis/summary_report.md', 'w') as f:
        f.write(report_content)
    
    with open('/tmp/netflix_analysis/complete_analysis.json', 'w') as f:
        json.dump(summary_data, f, indent=2)
    
    print("Summary report generated successfully!")

# Định nghĩa DAG
with DAG(
    'netflix_data_analysis',
    default_args=default_args,
    description='DAG phân tích dữ liệu Netflix Shows và tạo báo cáo',
    schedule='@daily',
    catchup=False,
    tags=['netflix', 'data_analysis', 'reporting'],
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    # Task kiểm tra dữ liệu đã được xử lý
    check_processed_data = PythonOperator(
        task_id='check_processed_data',
        python_callable=check_processed_data_exists,
    )

    # Task phân tích content trends
    content_trends_analysis = PythonOperator(
        task_id='analyze_content_trends',
        python_callable=analyze_content_trends,
    )

    # Task phân tích ratings distribution
    ratings_analysis = PythonOperator(
        task_id='analyze_ratings_distribution',
        python_callable=analyze_ratings_distribution,
    )

    # Task phân tích countries và genres
    countries_genres_analysis = PythonOperator(
        task_id='analyze_countries_and_genres',
        python_callable=analyze_countries_and_genres,
    )

    # Task phân tích duration patterns
    duration_analysis = PythonOperator(
        task_id='analyze_duration_patterns',
        python_callable=analyze_duration_patterns,
    )

    # Task tạo summary report
    summary_report = PythonOperator(
        task_id='generate_summary_report',
        python_callable=generate_summary_report,
    )

    # Task archive results
    archive_results = BashOperator(
        task_id='archive_analysis_results',
        bash_command='''
        # Tạo thư mục archive với timestamp
        archive_dir="/tmp/netflix_archive/$(date +%Y%m%d_%H%M%S)"
        mkdir -p "$archive_dir"
        
        # Copy tất cả analysis results
        cp -r /tmp/netflix_analysis/* "$archive_dir/"
        
        # Tạo compressed archive
        cd /tmp/netflix_archive
        tar -czf "netflix_analysis_$(date +%Y%m%d_%H%M%S).tar.gz" "$(basename $archive_dir)"
        
        echo "Analysis results archived to: $archive_dir"
        ls -la "$archive_dir"
        ''',
    )

    end_task = EmptyOperator(
        task_id='end'
    )

    # Định nghĩa dependencies
    start_task >> check_processed_data
    
    # Các task phân tích có thể chạy song song
    check_processed_data >> [content_trends_analysis, ratings_analysis, countries_genres_analysis, duration_analysis]
    
    # Tất cả phân tích phải hoàn thành trước khi tạo summary
    [content_trends_analysis, ratings_analysis, countries_genres_analysis, duration_analysis] >> summary_report
    
    summary_report >> archive_results >> end_task
