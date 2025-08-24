# Netflix Data Pipeline với Apache Airflow

Dự án này bao gồm một bộ DAGs để xử lý và phân tích dataset Netflix Shows từ Kaggle.

## Cấu trúc Pipeline

### 1. Data Ingestion DAG (`netflix_data_ingestion.py`)
- **Mục đích**: Tải xuống và validate dataset từ Kaggle
- **Schedule**: Hàng ngày
- **Tasks**:
  - Cài đặt dependencies (kaggle, pandas, numpy)
  - Download dataset từ Kaggle API
  - Validate dữ liệu
  - Backup dữ liệu thô

### 2. Data Processing DAG (`netflix_data_processing.py`)
- **Mục đích**: Làm sạch và xử lý dữ liệu
- **Schedule**: Hàng ngày (sau khi ingestion hoàn thành)
- **Tasks**:
  - Kiểm tra file input
  - Làm sạch dữ liệu (xử lý missing values, chuẩn hóa)
  - Tạo features mới (age_years, content_category, etc.)
  - Tách dữ liệu theo loại (Movies vs TV Shows)
  - Data quality check

### 3. Data Analysis DAG (`netflix_data_analysis.py`)
- **Mục đích**: Phân tích dữ liệu và tạo báo cáo
- **Schedule**: Hàng ngày (sau khi processing hoàn thành)
- **Tasks**:
  - Phân tích xu hướng nội dung theo thời gian
  - Phân tích phân phối ratings
  - Phân tích quốc gia và thể loại phổ biến
  - Phân tích patterns về duration
  - Tạo summary report
  - Archive kết quả

### 4. Master Pipeline DAG (`netflix_master_pipeline.py`)
- **Mục đích**: Orchestrate toàn bộ pipeline
- **Schedule**: Hàng tuần
- **Tasks**:
  - Setup môi trường
  - Trigger và monitor các DAGs con
  - Tạo pipeline summary report
  - Cleanup files cũ

## Setup và Cấu hình

### 1. Cài đặt Dependencies
```bash
pip install -r requirements.txt
```

### 2. Cấu hình Kaggle API
- Tạo account Kaggle và lấy API key
- Đặt file `kaggle.json` trong `~/.kaggle/`
- Hoặc set environment variables:
  ```bash
  export KAGGLE_USERNAME=your_username
  export KAGGLE_KEY=your_api_key
  ```

### 3. Khởi động Airflow
```bash
# Khởi tạo database
airflow db init

# Tạo admin user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# Start webserver và scheduler
airflow webserver --port 8080
airflow scheduler
```

## Cấu trúc Dữ liệu

### Input Dataset (từ Kaggle)
- **File**: `netflix_titles.csv`
- **Columns**: show_id, type, title, director, cast, country, date_added, release_year, rating, duration, listed_in, description

### Processed Data
- **netflix_titles_cleaned.csv**: Dữ liệu đã làm sạch
- **netflix_titles_features.csv**: Dữ liệu với features mới
- **netflix_movies.csv**: Chỉ movies
- **netflix_tv_shows.csv**: Chỉ TV shows

### Analysis Results
- **content_trends.json**: Xu hướng nội dung theo thời gian
- **ratings_analysis.json**: Phân tích ratings
- **countries_genres_analysis.json**: Phân tích quốc gia và thể loại
- **duration_patterns.json**: Patterns về thời lượng
- **summary_report.md**: Báo cáo tổng hợp

## Monitoring và Logging

### Logs Location
- Airflow logs: `logs/`
- Pipeline logs: `/tmp/netflix_*`

### Monitoring
- Airflow Web UI: `http://localhost:8080`
- DAG status và task execution
- Data quality reports

## Customization

### Thêm Analysis Mới
1. Tạo function analysis mới trong `netflix_data_analysis.py`
2. Thêm PythonOperator task
3. Update dependencies

### Thay đổi Schedule
- Modify `schedule_interval` trong DAG definition
- Có thể dùng: `@daily`, `@weekly`, `@monthly`, hoặc cron expression

### Thêm Data Sources
1. Modify `netflix_data_ingestion.py`
2. Thêm download tasks cho sources mới
3. Update validation logic

## Troubleshooting

### Common Issues
1. **Kaggle API errors**: Kiểm tra API credentials
2. **File not found**: Kiểm tra file paths và permissions
3. **Memory errors**: Tăng resources cho Airflow workers
4. **Import errors**: Cài đặt đầy đủ dependencies

### Log Analysis
```bash
# Xem logs của DAG cụ thể
tail -f logs/dag_id/task_id/execution_date/1.log

# Kiểm tra Airflow scheduler logs
tail -f logs/scheduler/latest/scheduler.log
```

## Mở rộng

### Tích hợp Database
- Thêm connections đến PostgreSQL/MySQL
- Lưu processed data vào database thay vì files
- Sử dụng SqlOperator cho complex queries

### Thêm Visualization
- Tích hợp với Plotly/Matplotlib
- Tạo dashboard với Streamlit/Dash
- Export charts cùng với analysis

### Alert và Notification
- Cấu hình email alerts
- Slack notifications
- Custom alert rules

## Contributors
- Data Engineer Team
- Data Analyst Team

## License
MIT License
