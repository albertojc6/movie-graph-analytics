from airflow.operators.python import PythonOperator # type: ignore
from airflow import DAG # type: ignore

# Relative imports from the same directory
from .population_functions import (
    to_graph_IMDB_crew,
    to_graph_IMDB_name,
    to_graph_IMDB_title,
    to_graph_MT_movies,
    to_graph_MT_rating,
    to_graph_TMDB
)

def create_tasks(dag: DAG):
    """
    Creates tasks to populate the Knowledge Graph in GraphDB.
    """

    IMDB_crew_task = PythonOperator(
        task_id='graph_IMDB_crew',
        python_callable=to_graph_IMDB_crew,
        dag=dag
    )

    IMDB_name_task = PythonOperator(
        task_id='graph_IMDB_name',
        python_callable=to_graph_IMDB_name,
        dag=dag
    )

    IMDB_title_task = PythonOperator(
        task_id='graph_IMDB_title',
        python_callable=to_graph_IMDB_title,
        dag=dag
    )

    TMDB_task = PythonOperator(
        task_id='graph_TMDB',
        python_callable=to_graph_TMDB,
        dag=dag
    )

    MT_movies_task = PythonOperator(
        task_id='graph_MT_movies',
        python_callable=to_graph_MT_movies,
        dag=dag
    )

    MT_ratings_task = PythonOperator(
        task_id='graph_MT_ratings',
        python_callable=to_graph_MT_rating,
        dag=dag
    )

    return IMDB_crew_task, IMDB_name_task, IMDB_title_task, TMDB_task, MT_movies_task, MT_ratings_task