SELECT
    nullif(trim(application_id),'') as application_id,
    nullif(trim(job_req_id),'') as job_req_id,
    nullif(trim(candidate_id),'') as candidate_id,
    trim(candidate_name) as candidate_name,
    trim(application_status) as application_status,
    trim(candidate_country) as candidate_country,
    try_to_date(application_date, 'YYYY-MM-DD') as application_date,
    try_to_date(snapshot_date, 'YYYY-MM-DD') as snapshot_date
FROM {{ source('ta_raw', 'applications')}}
WHERE snapshot_date = ' {{ var("ta_snapshot_date") }}'