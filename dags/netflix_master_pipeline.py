from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_pipeline_master',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa Master DAG để orchestrate toàn bộ pipeline
with DAG(
    'netflix_master_pipeline',
    default_args=default_args,
    description='Master DAG để orchestrate toàn bộ Netflix data pipeline',
    schedule='@weekly',  # Chạy hàng tuần
    catchup=False,
    tags=['netflix', 'master', 'pipeline', 'orchestration'],
) as dag:

    start_pipeline = EmptyOperator(
        task_id='start_pipeline'
    )

    # Chuẩn bị môi trường
    setup_environment = BashOperator(
        task_id='setup_environment',
        bash_command='''
        echo "Setting up Netflix data pipeline environment..."
        
        # Tạo các thư mục cần thiết
        mkdir -p /tmp/netflix_data
        mkdir -p /tmp/netflix_analysis
        mkdir -p /tmp/netflix_backup
        mkdir -p /tmp/netflix_archive
        
        # Kiểm tra disk space
        df -h /tmp
        
        echo "Environment setup completed!"
        ''',
    )

    # Trigger Data Ingestion DAG
    trigger_ingestion = TriggerDagRunOperator(
        task_id='trigger_data_ingestion',
        trigger_dag_id='netflix_data_ingestion',
        wait_for_completion=True,
        poke_interval=30,  # Check every 30 seconds instead of 60
        allowed_states=['success'],
        failed_states=['failed'],
    )

    # Trigger Data Processing DAG
    trigger_processing = TriggerDagRunOperator(
        task_id='trigger_data_processing',
        trigger_dag_id='netflix_data_processing',
        wait_for_completion=True,
        poke_interval=30,  # Check every 30 seconds
        allowed_states=['success'],
        failed_states=['failed'],
    )

    # Trigger Data Analysis DAG
    trigger_analysis = TriggerDagRunOperator(
        task_id='trigger_data_analysis',
        trigger_dag_id='netflix_data_analysis',
        wait_for_completion=True,
        poke_interval=30,  # Check every 30 seconds
        allowed_states=['success'],
        failed_states=['failed'],
    )

    # Tạo pipeline summary report
    generate_pipeline_summary = BashOperator(
        task_id='generate_pipeline_summary',
        bash_command='''
        echo "=== Netflix Data Pipeline Summary Report ===" > /tmp/netflix_pipeline_summary.txt
        echo "Generated on: $(date)" >> /tmp/netflix_pipeline_summary.txt
        echo "" >> /tmp/netflix_pipeline_summary.txt
        
        echo "Pipeline Execution Summary:" >> /tmp/netflix_pipeline_summary.txt
        echo "- Data Ingestion: Completed" >> /tmp/netflix_pipeline_summary.txt
        echo "- Data Processing: Completed" >> /tmp/netflix_pipeline_summary.txt
        echo "- Data Analysis: Completed" >> /tmp/netflix_pipeline_summary.txt
        echo "" >> /tmp/netflix_pipeline_summary.txt
        
        echo "Generated Files:" >> /tmp/netflix_pipeline_summary.txt
        if [ -d "/tmp/netflix_data" ]; then
            echo "Data files:" >> /tmp/netflix_pipeline_summary.txt
            ls -la /tmp/netflix_data/ >> /tmp/netflix_pipeline_summary.txt
        fi
        
        if [ -d "/tmp/netflix_analysis" ]; then
            echo "" >> /tmp/netflix_pipeline_summary.txt
            echo "Analysis files:" >> /tmp/netflix_pipeline_summary.txt
            ls -la /tmp/netflix_analysis/ >> /tmp/netflix_pipeline_summary.txt
        fi
        
        echo "" >> /tmp/netflix_pipeline_summary.txt
        echo "Pipeline completed successfully at $(date)" >> /tmp/netflix_pipeline_summary.txt
        
        # Display summary
        cat /tmp/netflix_pipeline_summary.txt
        ''',
    )

    # Cleanup task (optional)
    cleanup_temp_files = BashOperator(
        task_id='cleanup_temp_files',
        bash_command='''
        echo "Cleaning up temporary files older than 7 days..."
        
        # Cleanup old backup files (keep last 7 days)
        find /tmp/netflix_backup -name "*.csv" -mtime +7 -delete 2>/dev/null || true
        
        # Cleanup old archive files (keep last 30 days)  
        find /tmp/netflix_archive -name "*.tar.gz" -mtime +30 -delete 2>/dev/null || true
        
        echo "Cleanup completed!"
        ''',
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline'
    )

    # Định nghĩa dependencies cho Master Pipeline
    start_pipeline >> setup_environment >> trigger_ingestion
    trigger_ingestion >> trigger_processing  
    trigger_processing >> trigger_analysis
    trigger_analysis >> generate_pipeline_summary
    generate_pipeline_summary >> cleanup_temp_files >> end_pipeline
