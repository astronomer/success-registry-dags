drop table if exists {{ params.schema_name }}.{{ params.table_name }};
create table {{ params.schema_name }}.{{ params.table_name  }} (
  url varchar(64), --max 55
  id int, -- max 4
  external_id varchar, --max 0
  created_at datetime, --2021-08-18T16:50:10Z
  updated_at datetime,
  type varchar(64), --max 8
  subject varchar(256), --max 115
  raw_subject varchar(256), --max 115
  description text, --max 11319
  priority varchar(64), --max 4
  status varchar(64), --max 7
  recipient varchar(64), --max 21
  requester_id int, --max 13
  submitter_id int, --max 13
  assignee_id int, --max 13
  organization_id int, --max 13
  group_id int, --max 13
  collaborator_ids array, --max 238
  follower_ids array, --max 238
  email_cc_ids array, --max 0
  forum_topic_id varchar(256), --max 0
  problem_id int, --max 4
  has_incidents boolean,
  is_public boolean,
  due_at datetime, --empty, this is a guess
  tags array, --max 344
  custom_fields varchar(2000), --max 1154
  sharing_agreement_ids array, --max 2, this is a guess all empty arrays
  fields varchar(2000), --max 1154
  followup_ids array, --max 2, this is a guess all empty arrays
  ticket_form_id int, --max 13
  brand_id int, --max 13
  allow_channelback boolean,
  allow_attachments boolean,
  generated_timestamp timestamp, --unix timestamp
  "via.channel" varchar(64), --max 5
  "via.source.rel" varchar(64), --max 9
  "satisfaction_rating.score" varchar(64), --max 9
  "via.source.from.address" varchar(64), --max 36
  "via.source.from.name" varchar(64), --max 21
  "via.source.to.name" varchar(64), --max 18
  "via.source.to.address" varchar(64), --max 21
  "via.source.from.ticket_id" int, --max 4
  "via.source.from.subject" varchar(256), --max 69
  "via.source.from.channel" varchar(64), --max 3
  "satisfaction_rating.id" int, --max 13
  "satisfaction_rating.comment" text, --max 147
  "satisfaction_rating.reason" varchar(256), --max 18
  "satisfaction_rating.reason_id" varchar(256) --max 12
);
copy into {{ params.schema_name }}.{{ params.table_name }} from 's3://airflow-success/zendesk_extract/tickets/tickets_full_extract/all_tickets.csv'
credentials = (aws_key_id='{{ conn.my_conn_s3.extra_dejson.aws_access_key_id }}' aws_secret_key='{{ conn.my_conn_s3.extra_dejson.aws_secret_access_key }}')
file_format = (type = csv, record_delimiter= '\n' field_delimiter=',' field_optionally_enclosed_by='"' skip_header=1)