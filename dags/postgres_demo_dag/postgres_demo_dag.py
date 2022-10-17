"""
DAG that shows how to query Postgres via the PythonOperator by using the PostgresHook.

Users can also directly use the PostgresOperator.
"""

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def run_postgres_sql(**kwargs):
    sql = kwargs['templates_dict']['sql_file']
    pg_hook = PostgresHook(postgres_conn_id="airflow_db")
    conn = pg_hook.get_conn()
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()
    conn.close()


with DAG(
        dag_id='postgres_demo_dag',
        start_date=datetime(2021, 11, 30),
        max_active_runs=3,
        schedule_interval=None,
        template_searchpath="/usr/local/airflow/include/postgres_demo_dag/",
        catchup=False,
        doc_md=__doc__
    ) as dag:

    start = DummyOperator(task_id='start')
    finish = DummyOperator(task_id='finish')

    run_sql = PythonOperator(
        task_id='run_sql',
        python_callable=run_postgres_sql,
        provide_context=True,
        templates_dict={
            "sql_file": "sql/my_sql_file.sql"
        },
        templates_exts=['.sql',],
        params={
            "value_1": "parameter-value-1",
            "value_2": "parameter-value-2"
        }
    )

    start >> run_sql >> finish