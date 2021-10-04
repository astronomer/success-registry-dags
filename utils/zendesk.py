import datetime
import time
import pandas as pd
import requests
from airflow.hooks.base_hook import BaseHook
from utils.s3_automation import S3Tools

class ZendeskClient:
    def __init__(self):
        self.connection = BaseHook.get_connection("zendesk_api")
        self.domain = 'https://astronomer.zendesk.com'
        self.user = self.connection.login + '/token'
        self.pwd = self.connection.password

    def _upload_users_to_s3(self):
        S3 = S3Tools()
        df = self._get_users()
        S3._upload_zendesk_json_to_s3_as_csv(df=df, key="zendesk_extract/users/users.csv")

    def _get_users(self, ds='2015-01-01'):
        end_of_stream = False
        page_number = 1
        appended_data = []
        start_unix, end_unix = self._get_start_end_unix(ds=ds)
        url = f'/api/v2/incremental/users.json?start_time={int(start_unix)}'
        while end_of_stream is False:
            data = self._get_request(url)
            df = pd.json_normalize(data, record_path=['users'])
            appended_data.append(df)
            url = str(data['next_page']).replace(self.domain, '')
            end_of_stream = data['end_of_stream']
            print(f"Page {page_number} complete")
            page_number += 1
        df = pd.concat(appended_data)
        return df

    def _upload_tickets_to_s3(self, ds, key="out.csv", incremental=True):
        S3 = S3Tools()
        if incremental is True:
            df = self._get_tickets_ds(ds=ds)
            df = self._filter_and_sort_ticket_df(df)
        else:
            df = self._get_all_tickets()
            df = self._filter_and_sort_ticket_df(df)
        S3._upload_zendesk_json_to_s3_as_csv(df=df, key=key, replace=True)

    def _get_tickets_ds(self, ds):
        start_unix, end_unix = self._get_start_end_unix(ds=ds)
        endpoint = f"/api/v2/incremental/tickets.json?start_time={start_unix}&end_time={end_unix}"
        data = self._get_request(endpoint)
        return pd.json_normalize(data, record_path=['tickets'])

    def _get_all_tickets(self, ds='2015-01-01'):
        end_of_stream = False
        page_number = 1
        appended_data = []
        start_datetime_obj = datetime.datetime.combine(datetime.datetime.fromisoformat(ds), datetime.time.max)
        start_unix = time.mktime(start_datetime_obj.timetuple())
        url = f'/api/v2/incremental/tickets/cursor.json?start_time={start_unix}'
        while end_of_stream is False:
            data = self._get_request(url)
            df = pd.json_normalize(data, record_path=['tickets'])
            appended_data.append(df)
            url = f"/api/v2/incremental/tickets/cursor.json?cursor={data['after_cursor']}"
            end_of_stream = data['end_of_stream']
            print(f"Page {page_number} complete")
            page_number += 1
        df = pd.concat(appended_data)
        return df

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

    def _upload_organizations_to_s3(self):
        S3 = S3Tools()
        df = self._get_organizations()
        S3._upload_zendesk_json_to_s3_as_csv(df=df, key="zendesk_extract/organizations/organizations.csv")

    def _get_organizations(self, ds='2015-01-01'):
        end_of_stream = False
        page_number = 1
        appended_data = []
        start_unix, end_unix = self._get_start_end_unix(ds=ds)
        url = f'/api/v2/incremental/organizations.json?start_time={int(start_unix)}'
        while end_of_stream is False:
            data = self._get_request(url)
            df = pd.json_normalize(data, record_path=['organizations'])
            appended_data.append(df)
            url = str(data['next_page']).replace(self.domain, '')
            end_of_stream = data['end_of_stream']
            print(f"Page {page_number} complete")
            page_number += 1
        df = pd.concat(appended_data)
        return df

    def _get_start_end_unix(self, ds):
        start_datetime_obj = datetime.datetime.fromisoformat(ds)
        end_datetime_obj = datetime.datetime.combine(start_datetime_obj, datetime.time.max)
        start_unix = time.mktime(start_datetime_obj.timetuple())
        end_unix = time.mktime(end_datetime_obj.timetuple())
        return start_unix, end_unix

    def _query_zendesk_list(self, view_id):
        data = self._get_request(f"/api/v2/views/{view_id}/tickets.json")
        unique_zendesk_tickets = []
        ticket_list = data['tickets']
        for ticket in ticket_list:
            title = f"{ticket['id']} - {ticket['subject']}"
            zendesk_url = f"https://astronomer.zendesk.com/agent/tickets/{ticket['id']}"
            description = ticket['description']
            org = self._get_organization_name(int(ticket['organization_id']))
            custom_fields = ticket['custom_fields']  # 360026794033
            zendesk_priority = list(filter(lambda x: x['id'] == 360026794033, custom_fields))[0]['value']
            zendesk_priority = "NA" if zendesk_priority is None else zendesk_priority
            unique_zendesk_tickets.append({
                'Title': title,
                'Customer': org,
                'Zendesk_URL': zendesk_url,
                'Description': description,
                'Zendesk_Priority': zendesk_priority
            })

        return unique_zendesk_tickets

    def _get_organization_name(self, org_id):
        data = self._get_request(f"/api/v2/organizations/{org_id}.json")
        return data['organization']['name']

    def _get_ticket_descr(self, zendesk_url):
        ticket_id = zendesk_url.rsplit('/', 1)[-1]
        data = self._get_request(f"/api/v2/tickets/{ticket_id}.json")
        return data['ticket']['description']

    def _get_request(self, api_endpoint):
        url = self.domain + api_endpoint
        response = requests.get(url, auth=(self.user, self.pwd))
        if response.status_code != 200:
            raise ValueError(f' Response: {response.text}')
        else:
            data = response.json()
        return data