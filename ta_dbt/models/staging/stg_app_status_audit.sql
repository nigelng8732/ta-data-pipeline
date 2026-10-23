SELECT
    nullif(trim(application_id),'') as application_id,
    trim(application_status) as application_status,

    try_to_timestamp_tz(created_date) as created_date,
    created_date as source_created_date,
    
    try_to_date(snapshot_date, 'YYYY-MM-DD') as snapshot_date,
    source_filename,
    source_row_number,
    source_content_key,
    ingested_at

FROM {{ source('ta_raw', 'app_status_audit')}} as aps
WHERE snapshot_date = '{{ var("ta_snapshot_date") }}'

qualify row_number() over (
    partition by
        aps.application_id,
        aps.application_status,
        aps.created_date
    order by
        aps.source_filename,
        aps.source_row_number,
        aps.ingested_at
) = 1