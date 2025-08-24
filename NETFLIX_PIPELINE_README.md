# Netflix Data Pipeline

A comprehensive Apache Airflow pipeline for processing and analyzing Netflix Shows dataset from Kaggle. This pipeline demonstrates enterprise-grade data engineering practices with proper orchestration, error handling, and monitoring.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Pipeline Components](#pipeline-components)
- [Usage](#usage)
- [Data Flow](#data-flow)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [Best Practices](#best-practices)
- [Contributing](#contributing)

## 🎯 Overview

The Netflix Data Pipeline is a production-ready ETL/ELT solution that:

- **Ingests** Netflix Shows dataset from Kaggle
- **Processes** and cleans the raw data with feature engineering
- **Analyzes** content trends, ratings, geographical distribution, and duration patterns
- **Orchestrates** the entire workflow with proper dependency management
- **Monitors** data quality and pipeline health

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│                 │    │                 │    │                 │    │                 │
│  Data Ingestion │───▶│ Data Processing │───▶│ Data Analysis   │    │ Master Pipeline │
│                 │    │                 │    │                 │    │  (Orchestrator) │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
        │                        │                        │                        │
        ▼                        ▼                        ▼                        ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw Dataset   │    │ Cleaned & Features│    │ Analysis Results│    │   Pipeline      │
│   Validation    │    │   Enhanced Data   │    │   JSON Reports  │    │   Monitoring    │
│   Backup        │    │   Movies/TV Split │    │   Archives      │    │   Cleanup       │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
```

## ✨ Features

### Data Engineering Features
- **Automated Data Ingestion** from Kaggle API
- **Comprehensive Data Validation** and quality checks
- **Advanced Feature Engineering** with derived metrics
- **Parallel Processing** for optimal performance
- **Error Handling & Retry Logic** for robustness
- **Data Lineage Tracking** through file dependencies

### Analysis Features
- **Content Trends Analysis** - Production patterns over time
- **Ratings Distribution** - Content categorization and age demographics
- **Geographical Analysis** - Global content distribution
- **Duration Patterns** - Movie lengths and TV show seasons
- **Comprehensive Reporting** - JSON-formatted results

### Operational Features
- **Master Pipeline Orchestration** - Centralized workflow management
- **Environment Setup & Validation** - Prerequisites checking
- **Automated Cleanup** - Temporary file and backup management
- **Pipeline Monitoring** - Status tracking and notifications
- **Data Archiving** - Historical result preservation

## 🔧 Prerequisites

### System Requirements
- **Apache Airflow 3.0+**
- **Python 3.8+**
- **Docker & Docker Compose** (for containerized deployment)
- **4GB+ RAM** (recommended)
- **10GB+ disk space** (for data and backups)

### Python Dependencies
```python
pandas>=2.0.0
numpy>=1.24.0
kaggle>=1.5.0
apache-airflow>=3.0.0
```

### External Services
- **Kaggle Account** with API credentials configured
- **SMTP Server** (optional, for email notifications)

## 🚀 Installation

### 1. Clone Repository
```bash
git clone <repository-url>
cd netflix-data-pipeline
```

### 2. Environment Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\\Scripts\\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Docker Deployment (Recommended)
```bash
# Start Airflow services
docker-compose up -d

# Access Airflow UI
open http://localhost:8080
```

### 4. Kaggle Configuration
```bash
# Setup Kaggle API credentials
mkdir -p ~/.kaggle
cp kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
```

## ⚙️ Configuration

### Airflow Configuration
```python
# airflow.cfg key settings
[core]
dags_folder = /opt/airflow/dags
max_active_runs_per_dag = 1
parallelism = 32

[webserver]
base_url = http://localhost:8080
```

### Pipeline Configuration
```python
# Constants in each DAG
DATA_PATH = "/tmp/netflix_data"
ANALYSIS_PATH = "/tmp/netflix_analysis"
BACKUP_PATH = "/tmp/netflix_backup"
```

### Email Notifications (Optional)
```python
# In DEFAULT_ARGS
'email_on_failure': True,
'email': ['admin@company.com'],
'email_on_retry': False,
```

## 📦 Pipeline Components

### 1. Netflix Data Ingestion (`netflix_data_ingestion`)
**Purpose**: Download and validate Netflix dataset from Kaggle

**Tasks**:
- `start_ingestion` - Pipeline initialization
- `install_dependencies` - Setup required packages
- `download_netflix_dataset` - Download from Kaggle API
- `validate_dataset` - Data quality validation
- `backup_dataset` - Create timestamped backup
- `generate_quality_report` - Create ingestion report
- `end_ingestion` - Pipeline completion

**Output Files**:
- `netflix_titles.csv` - Raw dataset
- `ingestion_report.txt` - Quality report
- Backup in `/tmp/netflix_backup/YYYYMMDD_HHMMSS/`

### 2. Netflix Data Processing (`netflix_data_processing`)
**Purpose**: Clean, transform, and engineer features

**Tasks**:
- `start_processing` - Processing initialization
- `check_input_file` - Wait for ingestion completion
- `clean_netflix_data` - Data cleaning and preprocessing
- `create_derived_features` - Feature engineering
- `split_data_by_type` - Separate movies and TV shows
- `data_quality_check` - Generate processing report
- `end_processing` - Processing completion

**Key Features**:
- Text cleaning and standardization
- Date parsing and validation
- Duration parsing (movies: minutes, TV: seasons)
- Country and genre counting
- Content categorization by rating
- Decade-based grouping

**Output Files**:
- `netflix_titles_cleaned.csv` - Cleaned dataset
- `netflix_titles_features.csv` - Enhanced with features
- `netflix_movies.csv` - Movies only
- `netflix_tv_shows.csv` - TV shows only
- `processing_report.txt` - Processing summary

### 3. Netflix Data Analysis (`netflix_data_analysis`)
**Purpose**: Comprehensive analytical insights

**Tasks**:
- `start_analysis` - Analysis initialization
- `check_processed_data` - Wait for processing completion
- `analyze_content_trends` - Time-based trend analysis
- `analyze_ratings_distribution` - Content rating patterns
- `analyze_countries_and_genres` - Geographical distribution
- `analyze_duration_patterns` - Duration and seasons analysis
- `generate_summary_report` - Comprehensive summary
- `archive_analysis_results` - Archive with timestamp
- `end_analysis` - Analysis completion

**Analysis Output**:
- `content_trends.json` - Production trends over time
- `ratings_analysis.json` - Rating and category distribution
- `geographical_analysis.json` - Country and regional data
- `duration_patterns.json` - Movie/TV show duration insights
- `comprehensive_report.json` - Executive summary

### 4. Netflix Master Pipeline (`netflix_master_pipeline`)
**Purpose**: Orchestrate complete pipeline execution

**Tasks**:
- `start_pipeline` - Master orchestration start
- `setup_environment` - Environment preparation
- `validate_prerequisites` - System validation
- `trigger_data_ingestion` - Orchestrate ingestion DAG
- `trigger_data_processing` - Orchestrate processing DAG
- `trigger_data_analysis` - Orchestrate analysis DAG
- `generate_pipeline_summary` - Master summary report
- `cleanup_temp_files` - Cleanup and maintenance
- `pipeline_completion_notification` - Final notification
- `end_pipeline` - Master completion

## 🔄 Data Flow

### 1. Ingestion Phase
```
Kaggle API → Raw Dataset → Validation → Backup → Quality Report
```

### 2. Processing Phase
```
Raw Data → Cleaning → Feature Engineering → Type Splitting → Quality Check
```

### 3. Analysis Phase
```
Processed Data → Parallel Analysis → Result Generation → Archiving
```

### 4. Orchestration
```
Master Pipeline → Sequential DAG Triggers → Monitoring → Cleanup
```

## 📊 Usage

### Running Individual DAGs

#### 1. Data Ingestion Only
```bash
# Trigger ingestion DAG
airflow dags trigger netflix_data_ingestion

# Monitor progress
airflow dags state netflix_data_ingestion <execution_date>
```

#### 2. Full Pipeline (Recommended)
```bash
# Trigger master pipeline
airflow dags trigger netflix_master_pipeline

# Monitor in Airflow UI
open http://localhost:8080
```

### Manual Execution
```bash
# Test individual tasks
airflow tasks test netflix_data_ingestion download_netflix_dataset 2024-01-01

# Test entire DAG
airflow dags test netflix_data_processing 2024-01-01
```

### Accessing Results

#### Data Files
```bash
# View processed data
ls -la /tmp/netflix_data/
cat /tmp/netflix_data/processing_report.txt
```

#### Analysis Results
```bash
# View analysis results
ls -la /tmp/netflix_analysis/
python -m json.tool /tmp/netflix_analysis/comprehensive_report.json
```

#### Pipeline Summary
```bash
# View master pipeline summary
python -m json.tool /tmp/netflix_data/pipeline_summary.json
```

## 🔍 Monitoring & Troubleshooting

### Airflow UI Monitoring
1. **DAG View** - Overall pipeline status
2. **Graph View** - Task dependencies and status
3. **Gantt Chart** - Execution timeline
4. **Log View** - Detailed task logs

### Common Issues & Solutions

#### 1. Kaggle API Issues
```bash
# Check credentials
cat ~/.kaggle/kaggle.json

# Test API connection
kaggle datasets list --search netflix
```

#### 2. Disk Space Issues
```bash
# Check available space
df -h /tmp

# Clean old backups
find /tmp/netflix_backup -type d -mtime +7 -exec rm -rf {} +
```

#### 3. Memory Issues
```bash
# Monitor memory usage
docker stats

# Adjust Airflow worker memory
# Edit docker-compose.yml worker memory limits
```

#### 4. Import Errors
```bash
# Check Python dependencies
pip list | grep -E '(pandas|numpy|kaggle)'

# Reinstall if needed
pip install --upgrade pandas numpy kaggle
```

### Log Analysis
```bash
# Check DAG processor logs
tail -f /opt/airflow/logs/dag_processor/latest

# Check scheduler logs
tail -f /opt/airflow/logs/scheduler/latest

# Check specific task logs
tail -f /opt/airflow/logs/dag_id=*/run_id=*/task_id=*/attempt=*.log
```

## 📈 Performance Optimization

### Resource Allocation
```yaml
# docker-compose.yml optimization
services:
  airflow-worker:
    deploy:
      resources:
        limits:
          memory: 4G
          cpus: '2'
```

### Parallel Processing
```python
# Task parallelism in analysis DAG
check_processed_data >> [
    analyze_content_trends_task,
    analyze_ratings_distribution_task,
    analyze_countries_and_genres,
    analyze_duration_patterns_task
]
```

### Data Processing Optimization
```python
# Chunked processing for large datasets
chunk_size = 10000
for chunk in pd.read_csv(file_path, chunksize=chunk_size):
    # Process chunk
    pass
```

## 🔒 Security Best Practices

### 1. Credentials Management
- Store Kaggle API keys in Airflow Connections
- Use environment variables for sensitive data
- Rotate API keys regularly

### 2. Access Control
- Implement role-based access control in Airflow
- Restrict DAG modification permissions
- Monitor user activities

### 3. Data Security
- Encrypt data at rest
- Secure temporary file locations
- Implement data retention policies

## 🧪 Testing

### Unit Testing
```bash
# Test individual functions
python -m pytest tests/test_data_processing.py

# Test DAG structure
python -m pytest tests/test_dag_structure.py
```

### Integration Testing
```bash
# Test complete pipeline
airflow dags test netflix_master_pipeline 2024-01-01
```

### Data Quality Testing
```python
# Validate output data
def test_data_quality():
    df = pd.read_csv('/tmp/netflix_data/netflix_titles_features.csv')
    assert df.shape[0] > 1000  # Minimum record count
    assert df['duration_minutes'].notna().sum() > 0  # Duration parsing
```

## 📚 Best Practices

### 1. DAG Design
- Use clear, descriptive task IDs
- Implement proper error handling
- Add comprehensive documentation
- Design for idempotency

### 2. Data Management
- Implement data validation at each stage
- Create backup and recovery procedures
- Monitor data quality metrics
- Establish data retention policies

### 3. Monitoring
- Set up alerting for pipeline failures
- Monitor resource utilization
- Track data freshness metrics
- Implement SLA monitoring

### 4. Code Quality
- Follow PEP 8 style guidelines
- Use type hints for better documentation
- Implement comprehensive logging
- Write modular, reusable functions

## 🤝 Contributing

### Development Setup
```bash
# Clone repository
git clone <repository-url>
cd netflix-data-pipeline

# Setup development environment
python -m venv dev-env
source dev-env/bin/activate
pip install -r requirements-dev.txt

# Install pre-commit hooks
pre-commit install
```

### Code Standards
- Follow PEP 8 for Python code
- Add docstrings to all functions
- Include type hints where applicable
- Write comprehensive tests

### Pull Request Process
1. Create feature branch from main
2. Implement changes with tests
3. Update documentation as needed
4. Submit pull request with description
5. Address review feedback

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📞 Support

For support and questions:

- **Documentation**: Check this README and inline code documentation
- **Issues**: Create a GitHub issue for bugs or feature requests
- **Discussions**: Use GitHub Discussions for general questions

## 🏆 Acknowledgments

- **Apache Airflow** - Workflow orchestration platform
- **Kaggle** - Netflix Shows dataset provider
- **Pandas** - Data manipulation and analysis
- **Docker** - Containerization platform

---

**Version**: 1.0.0  
**Last Updated**: August 24, 2025  
**Maintainer**: Data Engineering Team
