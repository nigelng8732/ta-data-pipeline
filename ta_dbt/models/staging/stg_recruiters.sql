SELECT
    nullif(trim(recruiter_id),'') as recruiter_id,
    trim(recruiter_name) as recruiter_name,
    nullif(trim(recruiter_org_unit), '') as recruiter_org_unit,
    try_to_date(snapshot_date, 'YYYY-MM-DD') as snapshot_date,

    source_filename,
    source_row_number,
    source_content_key,
    ingested_at
FROM {{ source('ta_raw', 'recruiters')}} as r
WHERE snapshot_date = '{{ var("ta_snapshot_date") }}' 

qualify row_number() over (
    partition by
        r.recruiter_id,
        r.recruiter_name,
        r.recruiter_org_unit,
        r.snapshot_date
    order by
        r.source_filename,
        r.source_row_number,
        r.ingested_at
) = 1