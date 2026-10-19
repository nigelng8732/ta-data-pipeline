SELECT
    *
FROM {{ source('ta_raw', 'applications')}}