{% snapshot code_snapshot %}

{{
    config(
      target_schema='raw',
      strategy='timestamp',  
      updated_at='updated_at', 
      unique_key='lga_code',
    )
}}

select
    *,
    CURRENT_TIMESTAMP as updated_at
from {{ source('raw', 'code') }}

{% endsnapshot %}
