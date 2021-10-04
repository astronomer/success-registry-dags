import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3Tools:
    def __init__(self):
        self.s3_hook = S3Hook(aws_conn_id='my_conn_s3')
        self.bucket_name = 'airflow-success'

    def _upload_zendesk_json_to_s3_as_csv(self, df, key="out.csv", replace=True):
        with io.BytesIO() as in_mem_file:
            df.to_csv(in_mem_file, index=False)
            in_mem_file.seek(0)
            self.s3_hook._upload_file_obj(
                file_obj=in_mem_file,
                key=key,
                bucket_name=self.bucket_name,
                replace=replace
            )