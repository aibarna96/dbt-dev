{{
    config(
        unique_key='listing_id'
    )
}}

with

source as (
    select
        *,
        ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY dbt_valid_from) as row_num
    from {{ ref('listing_snapshot') }}
    where host_since is not null and host_since not like '%na%' and host_since not like '%NA%' 
),

common_neighbourhood as (
    select
        listing_id,
        listing_neighbourhood as most_common_neighbourhood
    from (
        select
            listing_id,
            listing_neighbourhood,
            ROW_NUMBER() OVER (PARTITION BY listing_id ORDER BY COUNT(listing_neighbourhood) DESC) as row_num
        from source
        group by listing_id, listing_neighbourhood
    ) ranked
    where row_num = 1
),

renamed as (
    select
        s. listing_id,
        scrape_id,
        scraped_date,
        host_id,
        host_name,
        TO_CHAR(TO_DATE(host_since, 'DD/MM/YYYY'), 'DD/MM/YYYY') as host_since,
        host_is_superhost,
        host_neighbourhood,
        s.listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        has_availability,
        availability_30,
        number_of_reviews,
        review_scores_rating,
        review_scores_accuracy,
        review_scores_cleanliness,
        review_scores_checkin,
        review_scores_communication,
        review_scores_value,
        case when row_num = 1 then '1900-01-01'::timestamp else s.dbt_valid_from end as dbt_valid_from,
        s.dbt_valid_to
    from source s
    join common_neighbourhood m on s.listing_id = m.listing_id
    where s.row_num = 1
)


select * from renamed
