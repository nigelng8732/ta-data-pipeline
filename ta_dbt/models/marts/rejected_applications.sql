select *
from {{ ref('int_applications_validated') }}
where not is_valid