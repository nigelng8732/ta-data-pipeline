SELECT
    nullif(trim(recruiter_id),'') as recruiter_id,
    trim(recruiter_name) as recruiter_name,
    nullif(trim(recruiter_org_unit), '') as recruiter_org_unit,
    try_to_date(snapshot_date, 'YYYY-MM-DD') as snapshot_date
FROM {{ source('ta_raw', 'recruiters')}}
WHERE snapshot_date = ' {{ var("ta_snapshot_date") }}' 