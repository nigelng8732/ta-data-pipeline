with validated as (
    select
        a.*,
        jr.rejection_reason as requisition_rejection_reason,
        audit.latest_valid_audit_status,
        audit.latest_valid_audit_timestamp,
        coalesce(audit.valid_event_count, 0) as valid_event_count,
        coalesce(audit.rejected_event_count, 0) as rejected_event_count,

        case
            when a.candidate_id is null
                then 'Missing candidate ID'

            when a.application_date is null
                then 'Missing or invalid application timestamp'

            when a.application_date::date > a.snapshot_date
                then 'Application after snapshot date'

            when a.application_status is null
                or a.application_status not in (
                    'New Application',
                    'Screening',
                    'Hiring Manager Interview',
                    'Interview',
                    'Offer',
                    'Hired'
                )
                then 'Invalid application status'

            when jr.job_req_id is null
                then 'Missing or unknown requisition'

            when jr.is_valid = false
                then 'Requisition failed validation'

            when coalesce(audit.rejected_event_count, 0) > 0
                then 'Application has rejected audit events'

            when coalesce(audit.valid_event_count, 0) = 0
                then 'No valid audit history'

            when a.application_status <> audit.latest_valid_audit_status
                then 'Application status differs from audit history'

        end as rejection_reason

    from {{ ref('stg_applications') }} as a

    left join {{ ref('int_requisitions_validated') }} as jr
        on a.job_req_id = jr.job_req_id
        and a.snapshot_date = jr.snapshot_date

    left join {{ ref('int_application_audit_summary') }} as audit
        on a.application_id = audit.application_id
        and a.snapshot_date = audit.snapshot_date
)

select
    *,
    rejection_reason is null as is_valid
from validated