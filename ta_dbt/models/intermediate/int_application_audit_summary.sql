with audit_quality as (
    select
        application_id,
        snapshot_date,
        count_if(is_valid) as valid_event_count,
        count_if(not is_valid) as rejected_event_count

    from {{ ref('int_app_status_audit_validated') }}

    group by application_id, snapshot_date
),

latest_valid_event as (
    select
        application_id,
        snapshot_date,
        application_status as latest_valid_audit_status,
        created_date as latest_valid_audit_timestamp

    from {{ ref('int_app_status_audit_validated') }}

    where is_valid

    qualify row_number() over (
        partition by application_id, snapshot_date
        order by created_date desc
    ) = 1
)

select
    quality.application_id,
    quality.snapshot_date,
    coalesce(quality.valid_event_count, 0) as valid_event_count,
    coalesce(quality.rejected_event_count, 0) as rejected_event_count,
    latest.latest_valid_audit_status,
    latest.latest_valid_audit_timestamp

from audit_quality as quality

left join latest_valid_event as latest
    on quality.application_id = latest.application_id
    and quality.snapshot_date = latest.snapshot_date