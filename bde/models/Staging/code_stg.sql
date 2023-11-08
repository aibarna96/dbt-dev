{{
    config(
        unique_key='lga_code'
    )
}}

with

source  as (

    select * from {{ ref('code_snapshot') }}

),

renamed as (
    select
        lga_code as lga_code,
        lga_name as lga_name,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as dbt_valid_from,
        dbt_valid_to
    from source
),

unknown as (
    select
        0 as lga_code,
        'unknown' as lga_name,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to

)
select * from unknown
union all
select * from renamed