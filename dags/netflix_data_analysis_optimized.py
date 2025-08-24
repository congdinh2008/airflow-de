"""
Netflix Data Analysis Pipeline

This DAG performs comprehensive analysis of Netflix Shows dataset including
content trends, ratings distribution, geographical analysis, and duration patterns.

Author: Data Analytics Team
Created: 2024-01-01
Last Modified: 2025-08-24
"""

import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# Constants
DATA_PATH = "/tmp/netflix_data"
ANALYSIS_PATH = "/tmp/netflix_analysis"
FEATURES_FILE = "netflix_titles_features.csv"

# Configuration
WAIT_TIMEOUT = 300  # 5 minutes
CHECK_INTERVAL = 30  # 30 seconds

# DAG Configuration
DEFAULT_ARGS = {
    "owner": "data_analyst",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
}


def wait_for_processed_data() -> str:
    """
    Wait for processed data to become available.
    
    Returns:
        str: Path to the processed features file
        
    Raises:
        FileNotFoundError: If file is not found within timeout
    """
    file_path = os.path.join(DATA_PATH, FEATURES_FILE)
    start_time = time.time()
    
    print(f"Waiting for processed data: {file_path}")
    
    while time.time() - start_time < WAIT_TIMEOUT:
        if os.path.exists(file_path):
            print(f"Processed data found: {file_path}")
            return file_path
        
        print(f"File not found, waiting... ({int(time.time() - start_time)}s elapsed)")
        time.sleep(CHECK_INTERVAL)
    
    raise FileNotFoundError(
        f"Processed data file {file_path} not found after {WAIT_TIMEOUT} seconds"
    )


def analyze_content_trends() -> Dict[str, any]:
    """
    Analyze content trends over time.
    
    Returns:
        Dict: Content trends analysis results
    """
    try:
        df = pd.read_csv(os.path.join(DATA_PATH, FEATURES_FILE))
        print("Analyzing content trends...")
        
        # Yearly content statistics
        yearly_stats = df.groupby(["release_year", "type"]).size().unstack(fill_value=0)
        
        # Decade-wise content distribution
        decade_stats = df.groupby(["decade", "type"]).size().unstack(fill_value=0)
        
        # Top production years
        top_years = df["release_year"].value_counts().head(10)
        
        # Content growth rate analysis
        yearly_totals = df.groupby("release_year").size()
        growth_rates = yearly_totals.pct_change().dropna()
        
        # Peak production periods
        peak_movie_year = df[df["type"] == "Movie"]["release_year"].mode().iloc[0]
        peak_tv_year = df[df["type"] == "TV Show"]["release_year"].mode().iloc[0]
        
        results = {
            "yearly_stats": yearly_stats.to_dict(),
            "decade_stats": decade_stats.to_dict(),
            "top_years": top_years.to_dict(),
            "growth_rates": growth_rates.tail(10).to_dict(),
            "peak_production": {
                "movies": int(peak_movie_year),
                "tv_shows": int(peak_tv_year)
            },
            "analysis_date": datetime.now().isoformat()
        }
        
        # Save results
        os.makedirs(ANALYSIS_PATH, exist_ok=True)
        with open(os.path.join(ANALYSIS_PATH, "content_trends.json"), "w") as f:
            json.dump(results, f, indent=2)
        
        print("Content trends analysis completed!")
        return results
        
    except Exception as e:
        print(f"Error in content trends analysis: {str(e)}")
        raise


def analyze_ratings_distribution() -> Dict[str, any]:
    """
    Analyze ratings and content category distribution.
    
    Returns:
        Dict: Ratings distribution analysis results
    """
    try:
        df = pd.read_csv(os.path.join(DATA_PATH, FEATURES_FILE))
        print("Analyzing ratings distribution...")
        
        # Rating distribution
        rating_dist = df["rating"].value_counts()
        
        # Content category distribution
        category_dist = df["content_category"].value_counts()
        
        # Rating by content type
        rating_by_type = df.groupby(["type", "rating"]).size().unstack(fill_value=0)
        
        # Content type ratio
        type_ratio = df["type"].value_counts(normalize=True) * 100
        
        # Age-based content analysis
        age_categories = {
            "Kids": ["TV-Y", "TV-Y7"],
            "Family": ["G", "PG", "TV-G", "TV-PG"],
            "Teen": ["PG-13", "TV-14"],
            "Adult": ["R", "NC-17", "TV-MA"]
        }
        
        age_distribution = {}
        for age_group, ratings in age_categories.items():
            count = df[df["rating"].isin(ratings)].shape[0]
            age_distribution[age_group] = count
        
        # Most popular ratings by decade
        popular_by_decade = (
            df.groupby(["decade", "rating"])
            .size()
            .groupby("decade")
            .idxmax()
            .apply(lambda x: x[1])
            .to_dict()
        )
        
        results = {
            "rating_distribution": rating_dist.to_dict(),
            "category_distribution": category_dist.to_dict(),
            "rating_by_type": rating_by_type.to_dict(),
            "type_ratio": type_ratio.to_dict(),
            "age_distribution": age_distribution,
            "popular_rating_by_decade": popular_by_decade,
            "analysis_date": datetime.now().isoformat()
        }
        
        # Save results
        with open(os.path.join(ANALYSIS_PATH, "ratings_analysis.json"), "w") as f:
            json.dump(results, f, indent=2)
        
        print("Ratings distribution analysis completed!")
        return results
        
    except Exception as e:
        print(f"Error in ratings analysis: {str(e)}")
        raise


def analyze_geographical_distribution() -> Dict[str, any]:
    """
    Analyze geographical distribution of content.
    
    Returns:
        Dict: Geographical analysis results
    """
    try:
        df = pd.read_csv(os.path.join(DATA_PATH, FEATURES_FILE))
        print("Analyzing geographical distribution...")
        
        # Clean and process country data
        country_data = []
        for countries in df["country"].dropna():
            if isinstance(countries, str):
                country_list = [c.strip() for c in countries.split(",")]
                country_data.extend(country_list)
        
        # Country content count
        country_counts = pd.Series(country_data).value_counts()
        top_countries = country_counts.head(20)
        
        # Multi-country productions
        multi_country = df[df["country_count"] > 1]
        multi_country_stats = {
            "total_multi_country": len(multi_country),
            "avg_countries": multi_country["country_count"].mean(),
            "max_countries": multi_country["country_count"].max()
        }
        
        # Regional analysis
        regional_mapping = {
            "North America": ["United States", "Canada", "Mexico"],
            "Europe": ["United Kingdom", "France", "Germany", "Spain", "Italy", "Netherlands"],
            "Asia": ["India", "Japan", "South Korea", "China", "Thailand", "Taiwan"],
            "South America": ["Brazil", "Argentina", "Colombia", "Chile"],
            "Others": []
        }
        
        regional_distribution = {}
        for region, countries in regional_mapping.items():
            if region != "Others":
                count = sum(country_counts.get(country, 0) for country in countries)
                regional_distribution[region] = count
        
        # Calculate "Others"
        accounted_total = sum(regional_distribution.values())
        regional_distribution["Others"] = country_counts.sum() - accounted_total
        
        results = {
            "top_countries": top_countries.to_dict(),
            "multi_country_stats": multi_country_stats,
            "regional_distribution": regional_distribution,
            "total_unique_countries": len(country_counts),
            "analysis_date": datetime.now().isoformat()
        }
        
        # Save results
        with open(os.path.join(ANALYSIS_PATH, "geographical_analysis.json"), "w") as f:
            json.dump(results, f, indent=2)
        
        print("Geographical distribution analysis completed!")
        return results
        
    except Exception as e:
        print(f"Error in geographical analysis: {str(e)}")
        raise


def analyze_duration_patterns() -> Dict[str, any]:
    """
    Analyze duration patterns for movies and TV shows.
    
    Returns:
        Dict: Duration patterns analysis results
    """
    try:
        df = pd.read_csv(os.path.join(DATA_PATH, FEATURES_FILE))
        print("Analyzing duration patterns...")
        
        # Movie duration analysis
        movies_df = df[df["type"] == "Movie"]
        valid_movie_durations = movies_df["duration_minutes"].dropna()
        
        if len(valid_movie_durations) > 0:
            movie_stats = {
                "avg_duration": float(valid_movie_durations.mean()),
                "median_duration": float(valid_movie_durations.median()),
                "min_duration": float(valid_movie_durations.min()),
                "max_duration": float(valid_movie_durations.max()),
                "std_duration": float(valid_movie_durations.std())
            }
            
            # Duration by decade for movies
            movies_by_decade = (
                movies_df.groupby("decade")["duration_minutes"]
                .agg(["mean", "count"])
                .dropna()
            )
            duration_by_decade = {
                "mean": movies_by_decade["mean"].to_dict(),
                "count": movies_by_decade["count"].to_dict()
            }
        else:
            movie_stats = {k: 0.0 for k in ["avg_duration", "median_duration", "min_duration", "max_duration", "std_duration"]}
            duration_by_decade = {"mean": {}, "count": {}}
        
        # TV show seasons analysis
        tv_shows_df = df[df["type"] == "TV Show"]
        valid_tv_seasons = tv_shows_df["seasons_count"].dropna()
        
        if len(valid_tv_seasons) > 0:
            tv_stats = {
                "avg_seasons": float(valid_tv_seasons.mean()),
                "median_seasons": float(valid_tv_seasons.median()),
                "min_seasons": float(valid_tv_seasons.min()),
                "max_seasons": float(valid_tv_seasons.max()),
                "single_season_shows": int((valid_tv_seasons == 1).sum())
            }
        else:
            tv_stats = {k: 0.0 for k in ["avg_seasons", "median_seasons", "min_seasons", "max_seasons", "single_season_shows"]}
        
        # Data quality metrics
        data_quality = {
            "movies_with_duration_data": len(valid_movie_durations),
            "tv_shows_with_seasons_data": len(valid_tv_seasons),
            "total_movies": len(movies_df),
            "total_tv_shows": len(tv_shows_df)
        }
        
        results = {
            "movie_duration_stats": movie_stats,
            "tv_seasons_stats": tv_stats,
            "movies_duration_by_decade": duration_by_decade,
            "data_quality": data_quality,
            "analysis_date": datetime.now().isoformat()
        }
        
        # Save results
        with open(os.path.join(ANALYSIS_PATH, "duration_patterns.json"), "w") as f:
            json.dump(results, f, indent=2)
        
        print("Duration patterns analysis completed!")
        print(f"Movies with duration data: {data_quality['movies_with_duration_data']}/{data_quality['total_movies']}")
        print(f"TV shows with seasons data: {data_quality['tv_shows_with_seasons_data']}/{data_quality['total_tv_shows']}")
        
        return results
        
    except Exception as e:
        print(f"Error in duration analysis: {str(e)}")
        raise


def generate_comprehensive_report() -> Dict[str, any]:
    """
    Generate comprehensive analysis summary report.
    
    Returns:
        Dict: Summary of all analysis results
    """
    try:
        print("Generating comprehensive analysis report...")
        
        # Load all analysis results
        analysis_files = [
            "content_trends.json",
            "ratings_analysis.json", 
            "geographical_analysis.json",
            "duration_patterns.json"
        ]
        
        summary = {
            "report_generated": datetime.now().isoformat(),
            "analyses_completed": [],
            "key_insights": {},
            "data_overview": {}
        }
        
        # Check which analyses were completed
        for filename in analysis_files:
            file_path = os.path.join(ANALYSIS_PATH, filename)
            if os.path.exists(file_path):
                summary["analyses_completed"].append(filename.replace(".json", ""))
        
        # Load dataset overview
        if os.path.exists(os.path.join(DATA_PATH, FEATURES_FILE)):
            df = pd.read_csv(os.path.join(DATA_PATH, FEATURES_FILE))
            summary["data_overview"] = {
                "total_records": len(df),
                "total_movies": len(df[df["type"] == "Movie"]),
                "total_tv_shows": len(df[df["type"] == "TV Show"]),
                "date_range": {
                    "earliest": int(df["release_year"].min()),
                    "latest": int(df["release_year"].max())
                },
                "unique_countries": df["country"].nunique(),
                "unique_ratings": df["rating"].nunique()
            }
        
        # Save comprehensive report
        with open(os.path.join(ANALYSIS_PATH, "comprehensive_report.json"), "w") as f:
            json.dump(summary, f, indent=2)
        
        print("Comprehensive report generated successfully!")
        return summary
        
    except Exception as e:
        print(f"Error generating comprehensive report: {str(e)}")
        raise


# DAG Definition
with DAG(
    dag_id="netflix_data_analysis_optimized",
    default_args=DEFAULT_ARGS,
    description="Comprehensive Netflix Shows dataset analysis pipeline",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["netflix", "data_analysis", "analytics", "insights"],
    doc_md=__doc__,
) as dag:

    # Start task
    start_analysis = EmptyOperator(
        task_id="start_analysis",
        doc_md="Analysis pipeline start marker"
    )

    # Wait for processed data
    check_processed_data = PythonOperator(
        task_id="check_processed_data",
        python_callable=wait_for_processed_data,
        doc_md="Wait for processed dataset to be available"
    )

    # Content trends analysis
    analyze_content_trends_task = PythonOperator(
        task_id="analyze_content_trends",
        python_callable=analyze_content_trends,
        doc_md="Analyze content production trends over time"
    )

    # Ratings distribution analysis
    analyze_ratings_distribution_task = PythonOperator(
        task_id="analyze_ratings_distribution",
        python_callable=analyze_ratings_distribution,
        doc_md="Analyze content ratings and categories distribution"
    )

    # Geographical analysis
    analyze_countries_and_genres = PythonOperator(
        task_id="analyze_countries_and_genres",
        python_callable=analyze_geographical_distribution,
        doc_md="Analyze geographical distribution of content"
    )

    # Duration patterns analysis
    analyze_duration_patterns_task = PythonOperator(
        task_id="analyze_duration_patterns",
        python_callable=analyze_duration_patterns,
        doc_md="Analyze duration patterns for movies and TV shows"
    )

    # Generate summary report
    generate_summary_report = PythonOperator(
        task_id="generate_summary_report",
        python_callable=generate_comprehensive_report,
        doc_md="Generate comprehensive analysis summary report"
    )

    # Archive results
    archive_analysis_results = BashOperator(
        task_id="archive_analysis_results",
        bash_command=f"""
        set -e
        ARCHIVE_DIR="{ANALYSIS_PATH}/archive/$(date +%Y%m%d_%H%M%S)"
        mkdir -p "$ARCHIVE_DIR"
        
        # Copy all analysis results to archive
        if [ -d "{ANALYSIS_PATH}" ]; then
            find {ANALYSIS_PATH} -name "*.json" -maxdepth 1 -exec cp {{}} "$ARCHIVE_DIR/" \\;
            echo "Analysis results archived to: $ARCHIVE_DIR"
            ls -la "$ARCHIVE_DIR"
        fi
        
        echo "Analysis pipeline completed successfully!"
        """,
        doc_md="Archive analysis results with timestamp"
    )

    # End task
    end_analysis = EmptyOperator(
        task_id="end_analysis",
        doc_md="Analysis pipeline completion marker"
    )

    # Define task dependencies
    start_analysis >> check_processed_data

    # Parallel analysis tasks
    check_processed_data >> [
        analyze_content_trends_task,
        analyze_ratings_distribution_task,
        analyze_countries_and_genres,
        analyze_duration_patterns_task
    ]

    # Sequential final tasks
    [
        analyze_content_trends_task,
        analyze_ratings_distribution_task,
        analyze_countries_and_genres,
        analyze_duration_patterns_task
    ] >> generate_summary_report >> archive_analysis_results >> end_analysis
