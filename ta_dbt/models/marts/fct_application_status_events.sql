select
    audit.application_id,
    audit.application_status,
    audit.created_date as status_changed_at,
    audit.snapshot_date
from {{ ref('int_app_status_audit_validated') }} as audit

inner join {{ ref('fct_applications') }} as app
    on audit.application_id = app.application_id
    and audit.snapshot_date = app.snapshot_date

where audit.is_valid