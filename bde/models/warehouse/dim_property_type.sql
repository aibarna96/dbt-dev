{{
    config(
        unique_key='property_type'
    )
}}

select  distinct property_type
from {{ ref('listing_stg') }} 
