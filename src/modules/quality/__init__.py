from airflow.operators.python import PythonOperator #type:ignore
from airflow import DAG # type: ignore

from utils.hdfs_utils import HDFSClient

from .MovieTweetings_TR import quality_MovieTweetings
from .IMDb_TR import quality_IMDb
from .TMDb_TR import quality_TMDb

def create_tasks(dag: DAG, hdfs_client: HDFSClient):
    """
    Creates the trusted zone tasks for all data sources.
    
    Args:
        dag: The DAG object
        hdfs_client: HDFSClient instance from MLPipeline
    """
    trusted_MovieTweetings = PythonOperator(
        task_id='trusted_MovieTweetings',
        python_callable=quality_MovieTweetings,
        op_kwargs={
            'hdfs_client': hdfs_client
        },
        dag=dag
    )

    trusted_IMDb = PythonOperator(
        task_id='trusted_IMDb',
        python_callable=quality_IMDb,
        op_kwargs={
            'hdfs_client': hdfs_client
        },
        dag=dag
    )

    trusted_TMDb = PythonOperator(
        task_id='trusted_TMDb',
        python_callable=quality_TMDb,
        op_kwargs={
            'hdfs_client': hdfs_client
        },
        dag=dag
    )

    return trusted_MovieTweetings, trusted_IMDb, trusted_TMDb