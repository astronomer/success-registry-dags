import json
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from utils.zendesk import ZendeskClient
from airflow.hooks.base import BaseHook

def get_aws_extra(extra_field_name):
    return BaseHook.get_connection("my_conn_s3").extra_dejson.get(extra_field_name)

with DAG(
        dag_id="zendesk_extract",
        start_date=datetime(2021, 9, 25),
        max_active_runs=1,
        schedule_interval=None,
        template_searchpath="/usr/local/airflow/include/sql/zendesk_extract/",
        catchup=True
    ) as dag:

    start = DummyOperator(
        task_id="start"
    )

    tickets_start = DummyOperator(
        task_id="tickets_start"
    )

    upload_daily_tickets_to_s3 = PythonOperator(
        task_id="upload_daily_tickets_to_s3",
        python_callable=ZendeskClient()._upload_tickets_to_s3,
        op_kwargs={
            "ds": "{{ds}}",
            "key": "zendesk_extract/tickets/{{ds}}/tickets.csv",
            "incremental": True
        }
    )

    extract_s3_daily_tickets_to_snowflake = SnowflakeOperator(
            task_id="copy_daily_tickets_to_snowflake",
            snowflake_conn_id="my_snowflake_conn",
            sql="{% include 'zendesk_tickets_daily.sql' %}",
            params={
                "schema_name": "sandbox_chronek",
                "table_name": "zendesk_tickets_daily",
                "aws_access_key_id": get_aws_extra("aws_access_key_id"),
                "aws_secret_access_key": get_aws_extra("aws_secret_access_key")
            },
            trigger_rule="all_success"
        )

    start_full_load = DummyOperator(
        task_id="start_full_load",
        trigger_rule="one_failed"
    )

    upload_full_tickets_to_s3 = PythonOperator(
        task_id="upload_full_tickets_to_s3",
        python_callable=ZendeskClient()._upload_tickets_to_s3,
        op_kwargs={
            "key": "zendesk_extract/tickets_full_extract/all_tickets.csv",
            "incremental": False
        }
    )

    full_ticket_extract_to_snowflake = SnowflakeOperator(
            task_id="copy_full_tickets_to_snowflake",
            snowflake_conn_id="my_snowflake_conn",
            sql="{% include 'zendesk_tickets.sql' %}",
            params={
                "schema_name": "sandbox_chronek",
                "table_name": "zendesk_tickets",
                "aws_access_key_id": get_aws_extra("aws_access_key_id"),
                "aws_secret_access_key": get_aws_extra("aws_secret_access_key")
            },
            trigger_rule="all_success"
        )

    tickets_finish = DummyOperator(
        task_id="tickets_finish",
        trigger_rule="one_success"
    )

    organizations_start = DummyOperator(
        task_id="organizations_start"
    )

    upload_organizations_to_s3 = PythonOperator(
        task_id="upload_organizations_to_s3",
        python_callable=ZendeskClient()._upload_organizations_to_s3,
        op_kwargs={
            "key": "zendesk_extract/organizations/organizations.csv"
        }
    )

    extract_s3_organizations_to_snowflake = SnowflakeOperator(
            task_id="copy_organizations_to_snowflake",
            snowflake_conn_id="my_snowflake_conn",
            sql="{% include 'zendesk_organizations.sql' %}",
            params={
                "schema_name": "sandbox_chronek",
                "table_name": "zendesk_organizations",
                "aws_access_key_id": get_aws_extra("aws_access_key_id"),
                "aws_secret_access_key": get_aws_extra("aws_secret_access_key")
            },
            trigger_rule="all_success"
        )

    organizations_finish = DummyOperator(
        task_id="organizations_finish"
    )

    users_start = DummyOperator(
        task_id="users_start"
    )

    upload_users_to_s3 = PythonOperator(
        task_id="upload_users_to_s3",
        python_callable=ZendeskClient()._upload_users_to_s3,
        op_kwargs={
            "key": "zendesk_extract/users/users.csv"
        }
    )

    extract_s3_users_to_snowflake = SnowflakeOperator(
            task_id="copy_users_to_snowflake",
            snowflake_conn_id="my_snowflake_conn",
            sql="{% include 'zendesk_users.sql' %}",
            params={
                "schema_name": "sandbox_chronek",
                "table_name": "zendesk_users",
                "aws_access_key_id": get_aws_extra("aws_access_key_id"),
                "aws_secret_access_key": get_aws_extra("aws_secret_access_key")
            },
            trigger_rule="all_success"
        )

    users_finish = DummyOperator(
        task_id="users_finish"
    )

    finish = DummyOperator(
        task_id="finish"
    )


    start >> [tickets_start, organizations_start, users_start]
    tickets_start >> upload_daily_tickets_to_s3 >> extract_s3_daily_tickets_to_snowflake >> [start_full_load, tickets_finish]
    start_full_load >> upload_full_tickets_to_s3 >> full_ticket_extract_to_snowflake >> tickets_finish
    organizations_start >> upload_organizations_to_s3 >> extract_s3_organizations_to_snowflake >> organizations_finish
    users_start >> upload_users_to_s3 >> extract_s3_users_to_snowflake >> users_finish
    [tickets_finish, organizations_finish, users_finish] >> finish
