drop table if exists {{ params.schema_name }}.{{ params.table_name }};
create table if not exists {{ params.schema_name }}.{{ params.table_name }} like {{ params.schema_name }}.zendesk_organizations;
copy into {{ params.schema_name }}.{{ params.table_name }} FROM 's3://airflow-success/zendesk_extract/organizations/{{ds}}/organizations.csv'
credentials = (aws_key_id='{{ params.aws_access_key_id }}' aws_secret_key='{{ params.aws_secret_access_key }}')
file_format = (type = csv, record_delimiter= '\n' field_delimiter=',' field_optionally_enclosed_by='"' skip_header=1)
;

merge into {{ params.schema_name }}.zendesk_organizations using {{ params.schema_name }}.{{ params.table_name }}
  on zendesk_organizations.id = {{ params.table_name }}.id
  when matched then update set
    zendesk_organizations.url = {{ params.table_name }}.url,
    zendesk_organizations.id = {{ params.table_name }}.id,
    zendesk_organizations.name = {{ params.table_name }}.name,
    zendesk_organizations.shared_tickets = {{ params.table_name }}.shared_tickets,
    zendesk_organizations.shared_comments = {{ params.table_name }}.shared_comments,
    zendesk_organizations.external_id = {{ params.table_name }}.external_id,
    zendesk_organizations.created_at = {{ params.table_name }}.created_at,
    zendesk_organizations.updated_at = {{ params.table_name }}.updated_at,
    zendesk_organizations.domain_names = {{ params.table_name }}.domain_names,
    zendesk_organizations.details = {{ params.table_name }}.details,
    zendesk_organizations.notes = {{ params.table_name }}.notes,
    zendesk_organizations.group_id = {{ params.table_name }}.group_id,
    zendesk_organizations.tags = {{ params.table_name }}.tags,
    zendesk_organizations.deleted_at = {{ params.table_name }}.deleted_at,
    zendesk_organizations."organization_fields.account_manager" = {{ params.table_name }}."organization_fields.account_manager",
    zendesk_organizations."organization_fields.airflow_expertise" = {{ params.table_name }}."organization_fields.airflow_expertise",
    zendesk_organizations."organization_fields.astronomer_product" = {{ params.table_name }}."organization_fields.astronomer_product",
    zendesk_organizations."organization_fields.cloud_provider" = {{ params.table_name }}."organization_fields.cloud_provider",
    zendesk_organizations."organization_fields.customer_health" = {{ params.table_name }}."organization_fields.customer_health",
    zendesk_organizations."organization_fields.infra_expertise" = {{ params.table_name }}."organization_fields.infra_expertise",
    zendesk_organizations."organization_fields.support_level" = {{ params.table_name }}."organization_fields.support_level"
  when not matched then insert (
    url,
    id,
    name,
    shared_tickets,
    shared_comments,
    external_id,
    created_at,
    updated_at,
    domain_names,
    details,
    notes,
    group_id,
    tags,
    deleted_at,
    "organization_fields.account_manager",
    "organization_fields.airflow_expertise",
    "organization_fields.astronomer_product",
    "organization_fields.cloud_provider",
    "organization_fields.customer_health",
    "organization_fields.infra_expertise",
    "organization_fields.support_level"
  ) values (
    {{ params.table_name }}.url,
    {{ params.table_name }}.id,
    {{ params.table_name }}.name,
    {{ params.table_name }}.shared_tickets,
    {{ params.table_name }}.shared_comments,
    {{ params.table_name }}.external_id,
    {{ params.table_name }}.created_at,
    {{ params.table_name }}.updated_at,
    {{ params.table_name }}.domain_names,
    {{ params.table_name }}.details,
    {{ params.table_name }}.notes,
    {{ params.table_name }}.group_id,
    {{ params.table_name }}.tags,
    {{ params.table_name }}.deleted_at,
    {{ params.table_name }}."organization_fields.account_manager",
    {{ params.table_name }}."organization_fields.airflow_expertise",
    {{ params.table_name }}."organization_fields.astronomer_product",
    {{ params.table_name }}."organization_fields.cloud_provider",
    {{ params.table_name }}."organization_fields.customer_health",
    {{ params.table_name }}."organization_fields.infra_expertise",
    {{ params.table_name }}."organization_fields.support_level"
  )
;
