{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

WITH aggregated_images AS (
    SELECT 
        "_airbyte_ab_id", 
        json_agg(json_build_object('url', '{{ var("prestashop_url") }}' || id || '-large_default/image.jpg')) AS images
    FROM {{ var("table_prefix") }}_products_associations_images
    GROUP BY "_airbyte_ab_id"
), deduplicated_stock AS (
    SELECT DISTINCT ON (id) * FROM {{ var("table_prefix") }}_stock_availables
), deduplicated_suppliers AS (
    SELECT DISTINCT ON (id_product) * FROM {{ var("table_prefix") }}_product_suppliers
), deduplicated_tax_rules AS (
    SELECT DISTINCT ON (id_tax_rules_group) * FROM {{ var("table_prefix") }}_tax_rules
)

SELECT 
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_products".id ||
      'product' ||
      'prestashop'
    )  as id,
    'prestashop' as source,
    '{{ var("integration_id") }}'::uuid as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_products._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_products".id as external_id,
    "{{ var("table_prefix") }}_products".name::jsonb->0->>'value' as name,
    "{{ var("table_prefix") }}_products".description::jsonb->0->>'value' as description,
    "{{ var("table_prefix") }}_products".description_short::jsonb->0->>'value' as short_description,
    "{{ var("table_prefix") }}_products".reference as reference,
    "{{ var("table_prefix") }}_products".type as type,
    NULL as url,
    stock.quantity::float as quantity_available,
    "{{ var("table_prefix") }}_products".minimal_quantity::float as minimal_quantity,
    NULL::float as stock_status,
    images.images as images,
    '{}'::jsonb as tags,
    NULL::boolean as purchasable,
    NULL::float as regular_price,
    "{{ var("table_prefix") }}_products".wholesale_price as sale_price,
    "{{ var("table_prefix") }}_products".price::float as price,
    NULL::float as total_sales,
    "{{ var("table_prefix") }}_products".on_sale::boolean as on_sale,
    NULL as rate,
    NULL as sku,
    NULL as slug,
    "{{ var("table_prefix") }}_products".state as status,
    "{{ var("table_prefix") }}_products".is_virtual::boolean as virtual,
    "{{ var("table_prefix") }}_products".weight::float as weight,
    "{{ var("table_prefix") }}_products".ean13 as ean13,
    "{{ var("table_prefix") }}_products".height::float as height,
    "{{ var("table_prefix") }}_products".width::float as width,
    "{{ var("table_prefix") }}_products".location as location,
    md5('{{ var("integration_id") }}' || manu.id ) as manufacturer_id,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_products".id_category_default ||
      'category' ||
      'prestashop'
    ) as category_id,
    md5('{{ var("integration_id") }}' || prod_sup.id ) as supplier_id,
    md5('{{ var("integration_id") }}' || taxrules.id_tax ) as tax_id,
    "{{ var("table_prefix") }}_products".unity as unity 

FROM "{{ var("table_prefix") }}_products"

LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_products
ON _airbyte_raw_{{ var("table_prefix") }}_products._airbyte_ab_id = "{{ var("table_prefix") }}_products"._airbyte_ab_id

LEFT JOIN aggregated_images AS images
ON images._airbyte_ab_id = "{{ var("table_prefix") }}_products"._airbyte_ab_id

LEFT JOIN {{ var("table_prefix") }}_products_a__ions_stock_availables as pasa
ON pasa._airbyte_ab_id = "{{ var("table_prefix") }}_products"._airbyte_ab_id

LEFT JOIN deduplicated_stock as stock
ON stock.id::text = pasa.id::text

LEFT JOIN {{ var("table_prefix") }}_manufacturers as manu
ON "{{ var("table_prefix") }}_products".id_manufacturer = manu.id::text

LEFT JOIN deduplicated_suppliers as prod_sup
ON prod_sup.id_product = "{{ var("table_prefix") }}_products".id::text

LEFT JOIN deduplicated_tax_rules as taxrules
ON "{{ var("table_prefix") }}_products".id_tax_rules_group = taxrules.id_tax_rules_group

WHERE pasa.id_product_attribute = '0'
