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
      "{{ var("table_prefix") }}_orders".id ||
      'order' ||
      'prestashop'
    )  as id,
    'prestashop' as source,
    '{{ var("integration_id") }}'::uuid as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_orders._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_orders".id as external_id,
    "{{ var("table_prefix") }}_orders".reference as number,
    NULL as created_via,
    NULL as version,
    NULL as status,
    NULL as currency,
    "{{ var("table_prefix") }}_orders".total_discounts as discount_total,
    "{{ var("table_prefix") }}_orders".total_discounts_tax_excl as discount_tax,
    "{{ var("table_prefix") }}_orders".total_shipping as shipping_total,
    "{{ var("table_prefix") }}_orders".total_shipping_tax_excl as shipping_tax,
    NULL as cart_tax,
    "{{ var("table_prefix") }}_orders".total_paid as total,
    "{{ var("table_prefix") }}_orders".total_paid_tax_excl as total_tax,
    NULL::boolean as prices_include_tax,
    NULL as customer_note,
    NULL::jsonb as billing,
    NULL::jsonb as shipping,
    "{{ var("table_prefix") }}_orders".payment as payment_method,
    NULL as transaction_id,
    NULL::date as date_paid,
    NULL::date as date_completed,
    "{{ var("table_prefix") }}_orders".associations::jsonb as lines,
    NULL::jsonb as tax_lines,
    NULL::jsonb as shipping_lines,
    NULL::jsonb as fee_lines,
    NULL::jsonb as coupon_lines,
    NULL::jsonb as refunds,
    "{{ var("table_prefix") }}_orders".valid::boolean as paid,
    "{{ var("table_prefix") }}_orders".date_upd as service_date_updated,
    "{{ var("table_prefix") }}_orders".date_add as service_date_created,
    NULL as invoice_number,
    NULL::date as invoice_date,
    NULL as delivery_number,
    NULL::date as delivery_date,
    "{{ var("table_prefix") }}_orders".valid::boolean as valid,
    "{{ var("table_prefix") }}_orders".shipping_number as shipping_number,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_orders".id_customer ||
      'customer' ||
      'prestashop'
    ) as customer_id
FROM "{{ var("table_prefix") }}_orders"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_orders
ON _airbyte_raw_{{ var("table_prefix") }}_orders._airbyte_ab_id = "{{ var("table_prefix") }}_orders"._airbyte_ab_id