{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT
md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_product_options".id ||
      'option' ||
      'prestashop'
    )  as id,
    '{{ var("integration_id") }}'::uuid as integration_id,
    "{{ var("table_prefix") }}_product_options".id as external_id,
    "{{ var("table_prefix") }}_product_options".name::jsonb->0->>'value' as name
FROM "{{ var("table_prefix") }}_product_options"