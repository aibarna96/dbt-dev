{% snapshot suburb_snapshot %}

{{
    config(
      target_schema='raw',
      strategy='timestamp',  
      updated_at='updated_at', 
      unique_key='lga_name',
    )
}}

select
    *,
    CURRENT_TIMESTAMP as updated_at  
from {{ source('raw', 'suburb') }}

{% endsnapshot %}
