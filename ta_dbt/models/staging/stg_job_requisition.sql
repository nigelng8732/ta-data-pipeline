SELECT
    nullif(trim(job_req_id),'') as job_req_id,
    trim(job_title) as job_title,
    upper(trim(job_grade)) as job_grade,
    trim(business_area) as business_area,
    trim(country) as country,
    nullif(trim(recruiter_id), '') as recruiter_id,
    upper(trim(requisition_status)) as requisition_status,

    try_to_date(approved_date, 'YYYY-MM-DD') as approved_date,
    try_to_date(closed_date, 'YYYY-MM-DD') as closed_date,
    try_to_date(snapshot_date, 'YYYY-MM-DD') as snapshot_date,

    approved_date as source_approved_date,
    closed_date as source_closed_date,

    source_filename,
    source_row_number,
    source_content_key,
    ingested_at

FROM {{ source('ta_raw', 'job_requisitions')}} as jr
WHERE snapshot_date = '{{ var("ta_snapshot_date") }}'

qualify row_number() over (
    partition by
        jr.job_req_id,
        jr.job_title,
        jr.job_grade,
        jr.business_area,
        jr.country,
        jr.recruiter_id,
        jr.requisition_status,
        jr.approved_date,
        jr.closed_date
    order by
        jr.source_filename,
        jr.source_row_number,
        jr.ingested_at
) = 1