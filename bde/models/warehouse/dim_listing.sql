{{
    config(
        unique_key='listing_id'
    )
}}

select  listing_id,
        listing_neighbourhood
from {{ ref('listing_stg') }}