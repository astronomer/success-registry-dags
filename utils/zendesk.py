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

    def _upload_users_to_s3(self, key="out.csv"):
        df = self._get_all(object='users')
        self._upload_zendesk_json_to_s3_as_csv(df=df, key=key)

    def _upload_tickets_to_s3(self, ds, key="out.csv", incremental=True):
        if incremental is True:
            df = self._get_tickets_ds(ds=ds)
            df = self._filter_and_sort_ticket_df(df)
        else:
            df = self._get_all(object='tickets')
            df = self._filter_and_sort_ticket_df(df)
        self._upload_zendesk_json_to_s3_as_csv(df=df, key=key, replace=True)

    def _upload_organizations_to_s3(self):
        df = self._get_all(object='organizations')
        self._upload_zendesk_json_to_s3_as_csv(df=df, key="zendesk_extract/organizations/organizations.csv")

    def _get_all(self, object, ds='2015-01-01'):
        if object == 'users':
            endpoint = f'/api/v2/incremental/users.json'
        elif object == 'tickets':
            endpoint = f'/api/v2/incremental/tickets/cursor.json'
        elif object == 'organizations':
            endpoint = f'/api/v2/incremental/organizations.json'
        else:
            ValueError("Object not defined")
        end_of_stream = False
        page_number = 1
        appended_data = []
        start_unix, end_unix = self._get_start_end_unix(ds=ds)
        url = endpoint + f"?start_time={int(start_unix)}"
        while end_of_stream is False:
            data = self._get_request(url)
            df = pd.json_normalize(data, record_path=[object])
            appended_data.append(df)
            if object == 'users' or object == 'organizations':
                url = str(data['next_page'].replace(self.domain, ''))
            elif object == 'tickets':
                url = f"/api/v2/incremental/tickets/cursor.json?cursor={data['after_cursor']}"
            else:
                ValueError("Object not defined")
            end_of_stream = data['end_of_stream']
            print(f"Page {page_number} complete")
            page_number += 1
        df = pd.concat(appended_data)
        return df

    def _get_tickets_ds(self, ds):
        start_unix, end_unix = self._get_start_end_unix(ds=ds)
        endpoint = f"/api/v2/incremental/tickets.json?start_time={start_unix}&end_time={end_unix}"
        data = self._get_request(endpoint)
        return pd.json_normalize(data, record_path=['tickets'])

    def _filter_and_sort_ticket_df(self, df):
        cols = [
            "url",
            "id",
            "external_id",
            "created_at",
            "updated_at",
            "type",
            "subject",
            "raw_subject",
            "description",
            "priority",
            "status",
            "recipient",
            "requester_id",
            "submitter_id",
            "assignee_id",
            "organization_id",
            "group_id",
            "collaborator_ids",
            "follower_ids",
            "email_cc_ids",
            "forum_topic_id",
            "problem_id",
            "has_incidents",
            "is_public",
            "due_at",
            "tags",
            "custom_fields",
            "sharing_agreement_ids",
            "fields",
            "followup_ids",
            "ticket_form_id",
            "brand_id",
            "allow_channelback",
            "allow_attachments",
            "generated_timestamp",
            "via.channel",
            "via.source.rel",
            "satisfaction_rating.score",
            "via.source.from.address",
            "via.source.from.name",
            "via.source.to.name",
            "via.source.to.address",
            "via.source.from.ticket_id",
            "via.source.from.subject",
            "via.source.from.channel",
            "satisfaction_rating.id",
            "satisfaction_rating.comment",
            "satisfaction_rating.reason",
            "satisfaction_rating.reason_id"
        ]
        df.filter(items=cols)
        df = df.reindex(columns=cols)
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