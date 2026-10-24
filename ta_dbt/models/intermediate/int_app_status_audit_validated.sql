with validated as (
    select
        audit.*,

        case
            when audit.application_id is null
                then 'Missing application ID'

            when app.application_id is null
                then 'Unknown application'

            when audit.created_date is null
                then 'Missing or invalid status-change timestamp'

            when audit.application_status is null
                or audit.application_status not in (
                    'New Application',
                    'Screening',
                    'Hiring Manager Interview',
                    'Interview',
                    'Offer',
                    'Hired'
                )
                then 'Invalid application status'

            when app.application_date is null
                then 'Missing or invalid application timestamp'

            when audit.created_date < app.application_date
                then 'Status change before application'

            when audit.created_date::date > audit.snapshot_date
                then 'Status change after snapshot date'

        end as rejection_reason

    from {{ ref('stg_app_status_audit') }} as audit

    left join {{ ref('stg_applications') }} as app
        on audit.application_id = app.application_id
        and audit.snapshot_date = app.snapshot_date
)

select
    *,
    rejection_reason is null as is_valid
from validated