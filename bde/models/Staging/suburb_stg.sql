{{
    config(
        unique_key='lga_name'
    )
}}

with

source  as (

    select * from {{ ref('suburb_snapshot') }}

),

renamed as (
    select
        lga_name as lga_name,
        suburb_name as suburb_name,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as dbt_valid_from,
        dbt_valid_to
    from source
),

unknown as (
    select
        'NULL' as lga_name,
        'unknown' as suburb_name,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to

)
select * from unknown
union all
select * from renamed