import json
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.hooks.base import BaseHook

class SnowflakeClient:
    def __init__(self):
        self.snow_hook = SnowflakeHook('my_snowflake_conn')
        self.s3_extra = json.loads(str(BaseHook.get_connection("my_conn_s3").get_extra()))
        self.aws_access_key_id = self.s3_extra['aws_access_key_id']
        self.aws_secret_access_key = self.s3_extra['aws_secret_access_key']

    def sf_task(self, task_id, sql_file_path, trigger_rule='all_success', *args, **kwargs):
        fd = open(sql_file_path)
        sql = fd.read()
        fd.close()
        return SnowflakeOperator(
            task_id=task_id,
            snowflake_conn_id='my_snowflake_conn',
            sql=sql,
            parameters={
                "aws_access_key_id": self.aws_access_key_id,
                "aws_secret_access_key": self.aws_secret_access_key
            },
            trigger_rule=trigger_rule
        )