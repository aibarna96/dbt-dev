{{
    config(
        unique_key='host_id'
    )
}}

select  
host_id,
host_name,
host_since,
host_is_superhost,
host_neighbourhood
from {{ ref('listing_stg') }}