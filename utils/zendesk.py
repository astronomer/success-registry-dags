import datetime
import time
import pandas as pd
import requests
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

class ZendeskClient:
    def __init__(self):
        self.s3_hook = S3Hook(aws_conn_id='my_conn_s3')
        self.bucket_name = 'airflow-success'
        self.connection = BaseHook.get_connection("zendesk_api")
        self.domain = 'https://astronomer.zendesk.com'
        self.user = self.connection.login + '/token'
        self.pwd = self.connection.password

    def _upload_to_s3(self, zendesk_object, key="out.csv", ds=None, cols=None, incremental=False):
        if incremental is True:
            df = self._get_daily(ds=ds, zendesk_object=zendesk_object)
            df = self._filter_and_sort_df(df=df, columns=cols)
        else:
            df = self._get_all(zendesk_object=zendesk_object)
            df = self._filter_and_sort_df(df=df, columns=cols)
        self._upload_zendesk_json_to_s3_as_csv(df=df, key=key, replace=True)

    def _get_all(self, zendesk_object, ds='1970-01-01'):
        if zendesk_object == 'users' or zendesk_object == 'organizations':
            endpoint = f'/api/v2/incremental/{zendesk_object}.json'
        elif zendesk_object == 'tickets':
            endpoint = f'/api/v2/incremental/tickets/cursor.json'
        else:
            ValueError("Object not defined")
        end_of_stream = False
        page_number = 1
        appended_data = []
        start_unix, end_unix = self._get_start_end_unix(ds=ds)
        url = endpoint + f"?start_time={int(start_unix)}"
        while end_of_stream is False:
            data = self._get_request(url)
            df = pd.json_normalize(data, record_path=[zendesk_object])
            appended_data.append(df)
            if zendesk_object == 'users' or zendesk_object == 'organizations':
                url = str(data['next_page'].replace(self.domain, ''))
            elif zendesk_object == 'tickets':
                url = f"/api/v2/incremental/tickets/cursor.json?cursor={data['after_cursor']}"
            else:
                ValueError("Zendesk object not defined")
            end_of_stream = data['end_of_stream']
            print(f"Page {page_number} complete")
            page_number += 1
        df = pd.concat(appended_data)
        return df

    def _get_daily(self, ds, zendesk_object):
        start_unix, end_unix = self._get_start_end_unix(ds=ds)
        endpoint = f"/api/v2/incremental/{zendesk_object}.json?start_time={int(start_unix)}&end_time={int(end_unix)}"
        data = self._get_request(endpoint)
        df = pd.json_normalize(data, record_path=[zendesk_object])
        return df

    def _filter_and_sort_df(self, df, columns):
        df.filter(items=columns)
        df = df.reindex(columns=columns)
        return df

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

    def _get_start_end_unix(self, ds):
        start_datetime_obj = datetime.datetime.fromisoformat(ds)
        end_datetime_obj = datetime.datetime.combine(start_datetime_obj, datetime.time.max)
        start_unix = time.mktime(start_datetime_obj.timetuple())
        end_unix = time.mktime(end_datetime_obj.timetuple())
        return start_unix, end_unix

    def _get_request(self, api_endpoint):
        url = self.domain + api_endpoint
        response = requests.get(url, auth=(self.user, self.pwd))
        if response.status_code != 200:
            raise ValueError(f' Response: {response.text}')
        else:
            data = response.json()
        return data