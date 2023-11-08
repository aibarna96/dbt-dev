{{
    config(
        unique_key='listing_id'
    )
}}

with check_dimensions as
(select
	listing_id,
	TO_DATE(host_since, 'DD/MM/YYYY') as listing_date,
	case when host_id in (select distinct host_id from {{ ref('listing_stg') }}) then host_id else 0 end as host_id,
	case when listing_neighbourhood in (select distinct suburb_name from {{ ref('suburb_stg') }}) then listing_neighbourhood else NULL end as suburb_name,
	property_type,
    room_type,
    accommodates,
    price,
    has_availability,
    availability_30,
    host_is_superhost,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value,
    listing_neighbourhood
from {{ ref('listing_stg') }})

select
	a.listing_id,
	a.listing_date,
	a.host_id,
    a.suburb_name,
	a.property_type,
    a.room_type,
    a.accommodates,
    a.price,
    a.has_availability,
    a.availability_30,
    a.number_of_reviews,
    a.review_scores_rating,
    a.review_scores_accuracy,
    a.review_scores_cleanliness,
    a.review_scores_checkin,
    a.review_scores_communication,
    a.review_scores_value,
    a.listing_neighbourhood,
    a.host_is_superhost
from check_dimensions a
left join {{ ref('listing_stg') }} b  on a.listing_id = b.listing_id
--and a.date::timestamp >= b.dbt_valid_from and a.date::timestamp < coalesce(b.dbt_valid_to, '9999-12-31'::timestamp)
--and a.date::timestamp >= c.dbt_valid_from and a.date::timestamp < coalesce(c.dbt_valid_to, '9999-12-31'::timestamp)
left join {{ ref('suburb_stg') }} d  on a.suburb_name = d.suburb_name 
--and a.date::timestamp >= d.dbt_valid_from and a.date::timestamp < coalesce(d.dbt_valid_to, '9999-12-31'::timestamp)