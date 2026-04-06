from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
import pandas as pd
import traceback

from etl_books.scrapers.book_scraper import scrape_books
from etl_books.transform.book_transform import transform_data
from etl_books.load.book_load import create_table, load_data
from etl_books.utils import logger


default_args = {
    'owner': 'yash',
    'retries': 2, # automatically retries on failure
    'retry_delay': timedelta(minutes=2)
}


def scrape_task(ti):
    logger.write_log({'task': 'scrape', 'status': 'started'})

    try:
        data = asyncio.run(scrape_books())

        ti.xcom_push(key="raw_data", value=data) # pass data between tasks

        logger.write_log({'task': 'scrape',
            'status': 'completed', 'records': len(data)
        })

    except Exception as e:
        logger.write_log({'task': 'scrape',
            'stage': 'fatal', 'exception': str(e),
            'exception_type': type(e).__name__, 'trace': traceback.format_exc()
        })
        raise


def transform_task(ti):
    logger.write_log({'task': 'transform', 'status': 'started'})

    try:
        raw_data = ti.xcom_pull(task_ids="scrape", key="raw_data")

        df = transform_data(raw_data)

        ti.xcom_push(key="clean_data", value=df.to_dict(orient="records"))

        logger.write_log({
            'task': 'transform',
            'status': 'completed',
            'records': len(df)
        })

    except Exception as e:
        logger.write_log({
            'task': 'transform', 'stage': 'fatal',
            'exception': str(e), 'exception_type': type(e).__name__,
            'trace': traceback.format_exc()
        })
        raise


# tasks define: scrape(reading) -> transform (cleaning/ formating) -> load (storing)
def load_task(ti):
    logger.write_log({'task': 'load', 'status': 'started'})

    try:
        data = ti.xcom_pull(task_ids="transform", key="clean_data") # pass data between task

        df = pd.DataFrame(data)

        create_table()
        load_data(df)

        logger.write_log({'task': 'load',
            'status': 'completed', 'records': len(df)
        })

    except Exception as e:
        logger.write_log({'task': 'load',
            'stage': 'fatal', 'exception': str(e),
            'exception_type': type(e).__name__, 'trace': traceback.format_exc()
        })
        raise


with DAG(
    dag_id="etl_books_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily", # this schedules daily
    catchup=False,
    tags=["etl", "books"]
) as dag:

    # runs python function
    scrape = PythonOperator(
        task_id="scrape",
        python_callable=scrape_task
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task
    )

    scrape >> transform >> load