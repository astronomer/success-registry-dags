drop table if exists sandbox_chronek.zendesk_tickets_daily;
create table if not exists sandbox_chronek.zendesk_tickets_daily like sandbox_chronek.zendesk_tickets;
copy into sandbox_chronek.zendesk_tickets_daily FROM 's3://airflow-success/zendesk_extract/tickets/{{ds}}/tickets.csv'
credentials = (aws_key_id=%(aws_access_key_id)s aws_secret_key=%(aws_secret_access_key)s)
file_format = (type = csv, record_delimiter= '\n' field_delimiter=',' field_optionally_enclosed_by='"' skip_header=1)
;

merge into sandbox_chronek.zendesk_tickets using sandbox_chronek.zendesk_tickets_daily
  on zendesk_tickets.id = zendesk_tickets_daily.id
  when matched then update set
    zendesk_tickets.url = zendesk_tickets_daily.url,
    zendesk_tickets.external_id = zendesk_tickets_daily.external_id,
    zendesk_tickets.created_at = zendesk_tickets_daily.created_at,
    zendesk_tickets.updated_at = zendesk_tickets_daily.updated_at,
    zendesk_tickets.type = zendesk_tickets_daily.type,
    zendesk_tickets.subject = zendesk_tickets_daily.subject,
    zendesk_tickets.raw_subject = zendesk_tickets_daily.raw_subject,
    zendesk_tickets.description = zendesk_tickets_daily.description,
    zendesk_tickets.priority = zendesk_tickets_daily.priority,
    zendesk_tickets.status = zendesk_tickets_daily.status,
    zendesk_tickets.recipient = zendesk_tickets_daily.recipient,
    zendesk_tickets.requester_id = zendesk_tickets_daily.requester_id,
    zendesk_tickets.submitter_id = zendesk_tickets_daily.submitter_id,
    zendesk_tickets.assignee_id = zendesk_tickets_daily.assignee_id,
    zendesk_tickets.organization_id = zendesk_tickets_daily.organization_id,
    zendesk_tickets.group_id = zendesk_tickets_daily.group_id,
    zendesk_tickets.collaborator_ids = zendesk_tickets_daily.collaborator_ids,
    zendesk_tickets.follower_ids = zendesk_tickets_daily.follower_ids,
    zendesk_tickets.email_cc_ids = zendesk_tickets_daily.email_cc_ids,
    zendesk_tickets.forum_topic_id = zendesk_tickets_daily.forum_topic_id,
    zendesk_tickets.problem_id = zendesk_tickets_daily.problem_id,
    zendesk_tickets.has_incidents = zendesk_tickets_daily.has_incidents,
    zendesk_tickets.is_public = zendesk_tickets_daily.is_public,
    zendesk_tickets.due_at = zendesk_tickets_daily.due_at,
    zendesk_tickets.tags = zendesk_tickets_daily.tags,
    zendesk_tickets.custom_fields = zendesk_tickets_daily.custom_fields,
    zendesk_tickets.sharing_agreement_ids = zendesk_tickets_daily.sharing_agreement_ids,
    zendesk_tickets.fields = zendesk_tickets_daily.fields,
    zendesk_tickets.followup_ids = zendesk_tickets_daily.followup_ids,
    zendesk_tickets.ticket_form_id = zendesk_tickets_daily.ticket_form_id,
    zendesk_tickets.brand_id = zendesk_tickets_daily.brand_id,
    zendesk_tickets.allow_channelback = zendesk_tickets_daily.allow_channelback,
    zendesk_tickets.allow_attachments = zendesk_tickets_daily.allow_attachments,
    zendesk_tickets.generated_timestamp = zendesk_tickets_daily.generated_timestamp,
    zendesk_tickets."via.channel" = zendesk_tickets_daily."via.channel",
    zendesk_tickets."via.source.rel" = zendesk_tickets_daily."via.source.rel",
    zendesk_tickets."satisfaction_rating.score" = zendesk_tickets_daily."satisfaction_rating.score",
    zendesk_tickets."via.source.from.address" = zendesk_tickets_daily."via.source.from.address",
    zendesk_tickets."via.source.from.name" = zendesk_tickets_daily."via.source.from.name",
    zendesk_tickets."via.source.to.name" = zendesk_tickets_daily."via.source.to.name",
    zendesk_tickets."via.source.to.address" = zendesk_tickets_daily."via.source.to.address",
    zendesk_tickets."via.source.from.ticket_id" = zendesk_tickets_daily."via.source.from.ticket_id",
    zendesk_tickets."via.source.from.subject" = zendesk_tickets_daily."via.source.from.subject",
    zendesk_tickets."via.source.from.channel" = zendesk_tickets_daily."via.source.from.channel",
    zendesk_tickets."satisfaction_rating.id" = zendesk_tickets_daily."satisfaction_rating.id",
    zendesk_tickets."satisfaction_rating.comment" = zendesk_tickets_daily."satisfaction_rating.comment",
    zendesk_tickets."satisfaction_rating.reason" = zendesk_tickets_daily."satisfaction_rating.reason",
    zendesk_tickets."satisfaction_rating.reason_id" = zendesk_tickets_daily."satisfaction_rating.reason_id"
  when not matched then insert (
    url,
    id,
    external_id,
    created_at,
    updated_at,
    type,
    subject,
    raw_subject,
    description,
    priority,
    status,
    recipient,
    requester_id,
    submitter_id,
    assignee_id,
    organization_id,
    group_id,
    collaborator_ids,
    follower_ids,
    email_cc_ids,
    forum_topic_id,
    problem_id,
    has_incidents,
    is_public,
    due_at,
    tags,
    custom_fields,
    sharing_agreement_ids,
    fields,
    followup_ids,
    ticket_form_id,
    brand_id,
    allow_channelback,
    allow_attachments,
    generated_timestamp,
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
  ) values (
    zendesk_tickets_daily.url,
    zendesk_tickets_daily.id,
    zendesk_tickets_daily.external_id,
    zendesk_tickets_daily.created_at,
    zendesk_tickets_daily.updated_at,
    zendesk_tickets_daily.type,
    zendesk_tickets_daily.subject,
    zendesk_tickets_daily.raw_subject,
    zendesk_tickets_daily.description,
    zendesk_tickets_daily.priority,
    zendesk_tickets_daily.status,
    zendesk_tickets_daily.recipient,
    zendesk_tickets_daily.requester_id,
    zendesk_tickets_daily.submitter_id,
    zendesk_tickets_daily.assignee_id,
    zendesk_tickets_daily.organization_id,
    zendesk_tickets_daily.group_id,
    zendesk_tickets_daily.collaborator_ids,
    zendesk_tickets_daily.follower_ids,
    zendesk_tickets_daily.email_cc_ids,
    zendesk_tickets_daily.forum_topic_id,
    zendesk_tickets_daily.problem_id,
    zendesk_tickets_daily.has_incidents,
    zendesk_tickets_daily.is_public,
    zendesk_tickets_daily.due_at,
    zendesk_tickets_daily.tags,
    zendesk_tickets_daily.custom_fields,
    zendesk_tickets_daily.sharing_agreement_ids,
    zendesk_tickets_daily.fields,
    zendesk_tickets_daily.followup_ids,
    zendesk_tickets_daily.ticket_form_id,
    zendesk_tickets_daily.brand_id,
    zendesk_tickets_daily.allow_channelback,
    zendesk_tickets_daily.allow_attachments,
    zendesk_tickets_daily.generated_timestamp,
    zendesk_tickets_daily."via.channel",
    zendesk_tickets_daily."via.source.rel",
    zendesk_tickets_daily."satisfaction_rating.score",
    zendesk_tickets_daily."via.source.from.address",
    zendesk_tickets_daily."via.source.from.name",
    zendesk_tickets_daily."via.source.to.name",
    zendesk_tickets_daily."via.source.to.address",
    zendesk_tickets_daily."via.source.from.ticket_id",
    zendesk_tickets_daily."via.source.from.subject",
    zendesk_tickets_daily."via.source.from.channel",
    zendesk_tickets_daily."satisfaction_rating.id",
    zendesk_tickets_daily."satisfaction_rating.comment",
    zendesk_tickets_daily."satisfaction_rating.reason",
    zendesk_tickets_daily."satisfaction_rating.reason_id"
  )
;