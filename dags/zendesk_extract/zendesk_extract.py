import pathlib
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from utils.zendesk import ZendeskClient
from utils.snowflake_tools import SnowflakeClient

wd = pathlib.Path(__file__).parent.resolve()

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow'
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('zendesk_extract',
         start_date=datetime(2021, 9, 25),
         max_active_runs=1,
         # schedule_interval="@daily",
         schedule_interval=None,
         default_args=default_args,
         catchup=True
         ) as dag:

    start = DummyOperator(
        task_id='start'
    )

    tickets_start = DummyOperator(
        task_id='tickets_start'
    )

    upload_daily_tickets_to_s3 = PythonOperator(
        task_id="upload_daily_tickets_to_s3",
        python_callable=ZendeskClient()._upload_tickets_to_s3,
        op_kwargs={
            'ds': '{{ds}}',
            'key': "zendesk_extract/tickets/{{ds}}/tickets.csv",
            'incremental': True
        }
    )

    extract_s3_daily_tickets_to_snowflake = SnowflakeClient().sf_task(
        task_id='copy_daily_tickets_to_snowflake',
        sql_file_path=f"{wd}/sql/zendesk_tickets_daily.sql"
    )

    start_full_load = DummyOperator(
        task_id='start_full_load',
        trigger_rule='one_failed'
    )

    upload_full_tickets_to_s3 = PythonOperator(
        task_id="upload_full_tickets_to_s3",
        python_callable=ZendeskClient()._upload_tickets_to_s3,
        op_kwargs={
            'key': "zendesk_extract/tickets_full_extract/all_tickets.csv",
            'incremental': False
        }
    )

    full_ticket_extract_to_snowflake = SnowflakeClient().sf_task(
        task_id='copy_full_tickets_to_snowflake',
        sql_file_path=f"{wd}/sql/zendesk_tickets.sql"
    )

    tickets_finish = DummyOperator(
        task_id='tickets_finish',
        trigger_rule='one_success'
    )

    organizations_start = DummyOperator(
        task_id='organizations_start'
    )

    upload_organizations_to_s3 = PythonOperator(
        task_id="upload_organizations_to_s3",
        python_callable=ZendeskClient()._upload_organizations_to_s3
    )

    extract_s3_organizations_to_snowflake = SnowflakeClient().sf_task(
        task_id='copy_organizations_to_snowflake',
        sql_file_path=f"{wd}/sql/zendesk_organizations.sql"
    )

    organizations_finish = DummyOperator(
        task_id='organizations_finish'
    )

    users_start = DummyOperator(
        task_id='users_start'
    )

    upload_users_to_s3 = PythonOperator(
        task_id="upload_users_to_s3",
        python_callable=ZendeskClient()._upload_users_to_s3
    )

    extract_s3_users_to_snowflake = SnowflakeClient().sf_task(
        task_id='copy_users_to_snowflake',
        sql_file_path=f"{wd}/sql/zendesk_users.sql"
    )

    users_finish = DummyOperator(
        task_id='users_finish'
    )

    finish = DummyOperator(
        task_id='finish'
    )


    start >> [tickets_start, organizations_start, users_start]
    tickets_start >> upload_daily_tickets_to_s3 >> extract_s3_daily_tickets_to_snowflake >> [start_full_load, tickets_finish]
    start_full_load >> upload_full_tickets_to_s3 >> full_ticket_extract_to_snowflake >> tickets_finish
    organizations_start >> upload_organizations_to_s3 >> extract_s3_organizations_to_snowflake >> organizations_finish
    users_start >> upload_users_to_s3 >> extract_s3_users_to_snowflake >> users_finish
    tickets_finish >> finish
    organizations_finish >> finish
    users_finish >> finish



