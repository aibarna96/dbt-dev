{% snapshot listing_snapshot %}

{{
    config(
      target_schema='raw',
      strategy='timestamp', 
      updated_at='updated_at',  
      unique_key='listing_id',
    )
}}

select
    *,
    CURRENT_TIMESTAMP as updated_at  
from {{ source('raw', 'listing') }}

{% endsnapshot %}
