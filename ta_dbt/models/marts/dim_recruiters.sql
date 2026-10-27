select
    recruiter_id,
    recruiter_name,
    recruiter_org_unit,
    snapshot_date
from {{ ref('stg_recruiters') }}