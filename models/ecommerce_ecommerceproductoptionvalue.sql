{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT
md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_product_option_values".id ||
      'optionvalue' ||
      'prestashop'
    )  as id,
    '{{ var("integration_id") }}'::uuid as integration_id,
    "{{ var("table_prefix") }}_product_option_values".id as external_id,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_product_option_values".id_attribute_group ||
      'option' ||
      'prestashop'
    )  as option_id,
    "{{ var("table_prefix") }}_product_option_values".name::jsonb as name
FROM "{{ var("table_prefix") }}_product_option_values"