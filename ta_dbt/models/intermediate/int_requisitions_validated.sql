with validated as (
    select
        jr.*,

        case
            when jr.job_req_id is null
                then 'Missing requisition ID'

            when jr.snapshot_date is null
                then 'Invalid snapshot date'

            when r.recruiter_id is null
                then 'Missing or unknown recruiter'

            when jr.requisition_status is null
                or jr.requisition_status not in ('OPEN', 'CLOSED')
                then 'Invalid requisition status'

            when jr.approved_date is null
                then 'Missing or invalid approval date'

            when jr.approved_date > jr.snapshot_date
                then 'Approval date after snapshot date'

            when nullif(trim(jr.source_closed_date), '') is not null
                and jr.closed_date is null
                then 'Invalid closure date'

            when jr.requisition_status = 'CLOSED'
                and jr.closed_date is null
                then 'Closed requisition without closure date'

            when jr.closed_date < jr.approved_date
                then 'Closure date before approval date'

            when jr.closed_date > jr.snapshot_date
                then 'Closure date after snapshot date'

            when jr.requisition_status = 'OPEN'
                and jr.closed_date is not null
                then 'Open requisition with closure date'

        end as rejection_reason

    from {{ ref('stg_job_requisition') }} as jr

    left join {{ ref('stg_recruiters') }} as r
        on jr.recruiter_id = r.recruiter_id
        and jr.snapshot_date = r.snapshot_date
)

select
    *,
    rejection_reason is null as is_valid
from validated