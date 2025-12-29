from datetime import datetime, timedelta
from airflow import DAG
from dotenv import load_dotenv

# Import custom modules from the new structure
# Note: Ensure __init__.py files exist in the new folder structure
from dags.utils import HDFSClient
import dags.modules.ingestion as ingestion_stage
import dags.modules.refinement as refinement_stage
import dags.modules.quality as quality_stage
import dags.modules.semantic as semantic_stage
import dags.modules.analytics as analytics_stage

# Set environment variables
load_dotenv(dotenv_path='/opt/airflow/.env')

# Initializes shared HDFS Client
hdfs_client = HDFSClient()

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(seconds=10),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    'Movie_KG_Pipeline',  # Renamed for professional clarity
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['DataOps', 'Semantic_Web', 'Production'],
    default_view='graph',
    max_active_tasks=2
) as dag:

    # 1. Ingestion Stage (Landing)
    ingest_tasks = ingestion_stage.create_tasks(dag, hdfs_client)
    # Unpack tuple for dependency setting
    ingest_mt, ingest_imdb, ingest_tmdb = ingest_tasks

    # 2. Refinement Stage (Formatting)
    format_tasks = refinement_stage.create_tasks(dag, hdfs_client)
    format_mt, format_imdb, format_tmdb = format_tasks

    # 3. Quality Assurance Stage (Trusted)
    quality_tasks = quality_stage.create_tasks(dag, hdfs_client)
    quality_mt, quality_imdb, quality_tmdb = quality_tasks

    # 4. Semantic Knowledge Graph Stage (Exploitation)
    kg_tasks = semantic_stage.create_tasks(dag)
    # Unpack specific KG population tasks
    (imdb_crew, imdb_name, imdb_title, tmdb_node, mt_movies, mt_ratings) = kg_tasks

    # 5. Analytics Stage
    analysis_task = analytics_stage.create_tasks(dag)

    # --- Dependency Definitions ---

    # Ingestion flow
    [ingest_mt, ingest_imdb] >> ingest_tmdb

    # Process all data ONLY after final ingestion completes
    ingest_tmdb >> [format_mt, format_imdb, format_tmdb]

    # Independent processing chains per data source
    format_mt >> quality_mt >> [mt_ratings, mt_movies]
    format_imdb >> quality_imdb >> [imdb_title, imdb_crew, imdb_name]
    format_tmdb >> quality_tmdb >> [tmdb_node]

    # Analytics runs after the graph is fully populated
    [mt_ratings, mt_movies, imdb_title, imdb_crew, imdb_name, tmdb_node] >> analysis_task