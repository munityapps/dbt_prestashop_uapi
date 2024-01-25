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
      "{{ var("table_prefix") }}_categories".id ||
      'category' ||
      'prestashop'
    )  as id,
    'prestashop' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_categories._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_categories".id as external_id,
    "{{ var("table_prefix") }}_categories".name as name,
    "{{ var("table_prefix") }}_categories".name as slug,
    "{{ var("table_prefix") }}_categories".description as description, 
    "{{ var("table_prefix") }}_categories".date_add as created_date,
    "{{ var("table_prefix") }}_categories".date_upd as updated_date,
    "{{ var("table_prefix") }}_categories".position as sort_order,
    NULL  as template_suffix,
    "{{ var("table_prefix") }}_categories".nb_products_recursive::float as products_count,
    "{{ var("table_prefix") }}_categories".level_depth::float as level_depth,
    NULL as type,
    NULL as published_scope,
    "{{ var("table_prefix") }}_categories".active::boolean as active,
    CASE
      WHEN "{{ var("table_prefix") }}_categories".id_parent != '0' THEN
        md5(
          '{{ var("integration_id") }}' ||
          "{{ var("table_prefix") }}_categories".id_parent ||
          'category' ||
          'prestashop'
        )
      ELSE
        NULL
    END as parent_category_id
FROM "{{ var("table_prefix") }}_categories"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_categories
ON _airbyte_raw_{{ var("table_prefix") }}_categories._airbyte_ab_id = "{{ var("table_prefix") }}_categories"._airbyte_ab_id