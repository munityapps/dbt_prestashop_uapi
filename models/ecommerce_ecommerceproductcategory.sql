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
        "{{ var("table_prefix") }}_products".id ||
        "{{ var("table_prefix") }}_products".id_category_default ||
        'productcategory' ||
        'prestashop'
    )  as id,
    'prestashop' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    NULL::jsonb as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_products".id as external_id,
    md5(
        '{{ var("integration_id") }}' ||
        "{{ var("table_prefix") }}_products".id ||
        'product' ||
        'prestashop'
    ) as product_id,
    md5(
        '{{ var("integration_id") }}' ||
        "{{ var("table_prefix") }}_products".id_category_default ||
        'category' ||
        'prestashop'
    ) as category_id
FROM "{{ var("table_prefix") }}_products"
