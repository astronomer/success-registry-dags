from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from include.zendesk_extract.zendesk_api import ZendeskToS3Operator
from utils.zendesk_fields import ticket_cols, org_cols, user_cols

zendesk_extracts = [
    {"object_name": "tickets", "object_schema": ticket_cols},
    {"object_name": "organizations", "object_schema": org_cols},
    {"object_name": "users", "object_schema": user_cols}
]

with DAG(
        dag_id="zendesk_extract",
        start_date=datetime(2021, 9, 25),
        max_active_runs=1,
        schedule_interval=None,
        template_searchpath="/usr/local/airflow/include/zendesk_extract/",
        catchup=True
    ) as dag:

    start = DummyOperator(
        task_id="start"
    )

    finish = DummyOperator(
        task_id="finish"
    )

    for extract in zendesk_extracts:
        extract_start = DummyOperator(task_id=f"{extract['object_name']}_start")

        extract_daily_to_s3 = ZendeskToS3Operator(
            task_id=f"upload_daily_{extract['object_name']}_to_s3",
            ds="{{ ds }}",
            obj_name=extract['object_name'],
            cols=extract['object_schema'],
            is_incremental=True,
            s3_key=f"zendesk_extract/{extract['object_name']}/{{{{ ds }}}}/{extract['object_name']}.csv",
            zendesk_conn_id="zendesk_api",
            s3_conn_id="my_conn_s3",
            s3_bucket_name="airflow-success"
        )

        extract_daily_to_snowflake = SnowflakeOperator(
            task_id=f"copy_daily_{extract['object_name']}_to_snowflake",
            snowflake_conn_id="my_snowflake_conn",
            sql="sql/zendesk_{}_daily.sql".format(extract['object_name']),
            params={
                "schema_name": "sandbox_chronek",
                "table_name": f"zendesk_{extract['object_name']}_daily"
            },
            trigger_rule="all_success"
        )
        extract_full_load = DummyOperator(
            task_id=f"start_{extract['object_name']}_full_load",
            trigger_rule="one_failed"
        )

        extract_full_to_s3 = ZendeskToS3Operator(
            task_id=f"upload_full_{extract['object_name']}_to_s3",
            ds='{{ds}}',
            obj_name=extract['object_name'],
            cols=extract['object_schema'],
            is_incremental=False,
            s3_key=f"zendesk_extract/{extract['object_name']}/{extract['object_name']}_full_extract/all_{extract['object_name']}.csv",
            zendesk_conn_id="zendesk_api",
            s3_conn_id="my_conn_s3",
            s3_bucket_name="airflow-success"
        )

        extract_full_to_snowflake = SnowflakeOperator(
            task_id=f"copy_full_{extract['object_name']}_to_snowflake",
            snowflake_conn_id="my_snowflake_conn",
            sql="sql/zendesk_{}.sql".format(extract['object_name']),
            params={
                "schema_name": "sandbox_chronek",
                "table_name": f"zendesk_{extract['object_name']}"
            },
            trigger_rule="all_success"
        )
        extract_finish = DummyOperator(
            task_id=f"{extract['object_name']}_finish",
            trigger_rule="one_success"
        )

        '''
        extract_daily_to_snowflake has two downstream tasks. extract_full_load is triggered if the upstream 
        fails causing the table and schema to be reset. If the upstream succeeds, then the extract_full_load is skipped 
        and extract_finish is triggered
        '''

        start >> extract_start >> extract_daily_to_s3 >> extract_daily_to_snowflake
        extract_daily_to_snowflake >> [extract_full_load, extract_finish]
        extract_full_load >> extract_full_to_s3 >> extract_full_to_snowflake >> extract_finish
        extract_finish >> finish

