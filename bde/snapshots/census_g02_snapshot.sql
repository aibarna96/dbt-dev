{% snapshot census_g02_snapshot %}

{{
    config(
      target_schema='raw',
      strategy='timestamp', 
      updated_at='updated_at',  
      unique_key='lga_code_2016',
    )
}}

select
    *,
    CURRENT_TIMESTAMP as updated_at  
from {{ source('raw', 'census_g02') }}

{% endsnapshot %}
