SELECT
    *
FROM {{ source('ta_raw', 'job_requisitions')}}