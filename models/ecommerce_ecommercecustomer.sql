{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_customers".id ||
      'customer' ||
      'prestashop'
    )  as id,
    'prestashop' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_customers._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_customers".id as external_id,
    "{{ var("table_prefix") }}_customers".firstname as firstname,
    "{{ var("table_prefix") }}_customers".lastname as lastname,
    NULL as username,
    NULL::date AS birthday,
    "{{ var("table_prefix") }}_customers".email as email,
    NULL as address,
    NULL::boolean as email_marketing_consent,
    NULL::integer as order_count,
    NULL as state,
    NULL::float as total_spent,
    "{{ var("table_prefix") }}_customers".note as note,
    NULL as phone,
    NULL::jsonb as addresses,
    NULL as tags,
    NULL as role,
    NULL::boolean as tax_exempt,
    NULL::jsonb as billing,
    NULL::jsonb as shipping,
    NULL as avatar_url,
    "{{ var("table_prefix") }}_customers".active::boolean as active,
    "{{ var("table_prefix") }}_customers".is_guest::boolean as is_guest,
    "{{ var("table_prefix") }}_customers".date_upd as service_date_updated,
    "{{ var("table_prefix") }}_customers".date_add as service_date_created
FROM "{{ var("table_prefix") }}_customers"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_customers
ON _airbyte_raw_{{ var("table_prefix") }}_customers._airbyte_ab_id = "{{ var("table_prefix") }}_customers"._airbyte_ab_id
