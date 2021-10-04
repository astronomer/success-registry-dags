drop table if exists sandbox_chronek.zendesk_organizations;
create table sandbox_chronek.zendesk_organizations (
  url varchar(256), --max 70
  id int, --max 13
  name varchar(256), --max 32
  shared_tickets boolean,
  shared_comments boolean,
  external_id int, --a guess because they are all null
  created_at timestamp,
  updated_at timestamp,
  domain_names array, --max 55
  details varchar(256), --a guess because they are all null
  notes text, --max 190
  group_id int, --a guess because they are all null
  tags array, --max 126
  deleted_at timestamp,
  "organization_fields.account_manager" varchar(256), --max 12
  "organization_fields.airflow_expertise" varchar(256), --max 30
  "organization_fields.astronomer_product" varchar(256), --max 21
  "organization_fields.cloud_provider" varchar(256), --max 5
  "organization_fields.customer_health" varchar(256), --max 6
  "organization_fields.infra_expertise" varchar(256), --max 28
  "organization_fields.support_level" varchar(256) --max 15
);
copy into sandbox_chronek.zendesk_organizations FROM 's3://airflow-success/zendesk_extract/organizations/organizations.csv'
credentials = (aws_key_id=%(aws_access_key_id)s aws_secret_key=%(aws_secret_access_key)s)
file_format = (type = csv, record_delimiter= '\n' field_delimiter=',' field_optionally_enclosed_by='"' skip_header=1)