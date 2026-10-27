select
    job_req_id,
    job_title,
    job_grade,
    business_area,
    country,
    recruiter_id,
    requisition_status,
    approved_date,
    closed_date,
    snapshot_date
from {{ ref('int_requisitions_validated') }}
where is_valid