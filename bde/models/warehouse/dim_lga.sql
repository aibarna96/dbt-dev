{{
    config(
        unique_key='lga_code'
    )
}}

select  lga_code,
        lga_name
from {{ ref('code_stg') }}