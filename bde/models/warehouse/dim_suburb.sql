{{
    config(
        unique_key='suburb_name'
    )
}}

select  suburb_name
from {{ ref('suburb_stg') }}