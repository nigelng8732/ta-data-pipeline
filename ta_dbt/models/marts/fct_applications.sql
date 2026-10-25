select
    application_id,
    job_req_id,
    candidate_id,
    application_date,
    application_status,
    snapshot_date,
    latest_valid_audit_status
from {{ ref('int_applications_validated') }}
where is_valid = True