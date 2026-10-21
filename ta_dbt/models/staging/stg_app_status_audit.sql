SELECT
    nullif(trim(application_id),'') as application_id,
    trim(application_status) as application_status,
    try_to_date(created_date, 'YYYY-MM-DD') as created_date,
    try_to_date(snapshot_date, 'YYYY-MM-DD') as snapshot_date
FROM {{ source('ta_raw', 'app_status_audit')}}
WHERE snapshot_date = ' {{ var("ta_snapshot_date") }}'