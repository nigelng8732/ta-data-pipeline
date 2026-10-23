SELECT
    nullif(trim(application_id),'') as application_id,
    nullif(trim(job_req_id),'') as job_req_id,
    nullif(trim(candidate_id),'') as candidate_id,
    trim(candidate_name) as candidate_name,
    trim(application_status) as application_status,
    trim(candidate_country) as candidate_country,

    try_to_timestamp_tz(application_date) as application_date,
    application_date as source_application_date,
    
    try_to_date(snapshot_date, 'YYYY-MM-DD') as snapshot_date,

    source_filename,
    source_row_number,
    source_content_key,
    ingested_at

FROM {{ source('ta_raw', 'applications')}} as a
WHERE snapshot_date = '{{ var("ta_snapshot_date") }}'

qualify row_number() over (
    partition by
        a.application_id,
        a.job_req_id,
        a.candidate_id,
        a.candidate_name,
        a.application_status,
        a.candidate_country
    order by
        a.source_filename,
        a.source_row_number,
        a.ingested_at
) = 1