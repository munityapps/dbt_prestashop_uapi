{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT
    md5('{{ var("integration_id") }}' || comb.id || (elem ->>'id')::text) as id,
    ('{{ var("integration_id") }}')::uuid as integration_id,
    md5('{{ var("integration_id") }}' || comb.id || 'variation' || 'prestashop') AS variant_id,
    md5(
      '{{ var("integration_id") }}' ||
       (elem ->> 'id')::text||
      'option' ||
      'prestashop'
    )  AS option_id
from {{ var("table_prefix") }}_combinations comb
LEFT JOIN LATERAL jsonb_array_elements(comb.associations::jsonb->'product_option_values') AS elem ON true