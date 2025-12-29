from airflow.operators.python import PythonOperator # type: ignore
from airflow import DAG # type: ignore

from utils.hdfs_utils import HDFSClient
from .MovieTweetings_FR import format_MovieTweetings
from .IMDb_FR import format_IMDb
from .TMDb_FR import format_TMDb

def create_tasks(dag: DAG, hdfs_client: HDFSClient):

    format_MovieTweeting = PythonOperator(
        task_id='format_MovieTweetings',
        python_callable=format_MovieTweetings,
        op_kwargs={
            'hdfs_client': hdfs_client
        },
        dag=dag
    )

    format_IMDB = PythonOperator(
        task_id='format_IMDb',
        python_callable=format_IMDb,
        op_kwargs={
            'hdfs_client': hdfs_client
        },
        dag=dag
    )

    format_TMDB = PythonOperator(
        task_id='format_TMDb',
        python_callable=format_TMDb,
        op_kwargs={
            'hdfs_client': hdfs_client
        },
        dag=dag
    )

    return format_MovieTweeting, format_IMDB, format_TMDB