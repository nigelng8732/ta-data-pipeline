SELECT
    *
FROM {{ source('ta_raw', 'app_status_audit')}}