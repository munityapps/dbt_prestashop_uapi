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
      'product' ||
      'prestashop'
    )  as id,
    'prestashop' as source,
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_products._airbyte_data as last_raw_data, 
    '{{ var("timestamp") }}' as sync_timestamp,
    "{{ var("table_prefix") }}_products".id as external_id,
    "{{ var("table_prefix") }}_products".name::jsonb->0->>'value' as name,
    "{{ var("table_prefix") }}_products".description::jsonb->0->>'value' as description,
    "{{ var("table_prefix") }}_products".description_short::jsonb->0->>'value' as short_description ,
    "{{ var("table_prefix") }}_products".reference as reference,
    "{{ var("table_prefix") }}_products".type as type ,
    NULL as url,
    stock.quantity::float as quantity_available ,
    "{{ var("table_prefix") }}_products".minimal_quantity::float as minimal_quantity ,
    NULL::float as stock_status ,
    image_table.images as images,
    '{}'::jsonb  as tags,
    NULL::boolean as purchasable ,
    NULL::float as regular_price ,
    "{{ var("table_prefix") }}_products".wholesale_price as sale_price ,
    "{{ var("table_prefix") }}_products".price::float as price ,
    NULL::float as total_sales ,
    "{{ var("table_prefix") }}_products".on_sale::boolean as on_sale ,
    NULL as rate ,
    NULL as sku ,
    NULL as slug ,
    "{{ var("table_prefix") }}_products".state as status ,
    "{{ var("table_prefix") }}_products".is_virtual::boolean as virtual ,
    "{{ var("table_prefix") }}_products".weight::float as weight ,
    "{{ var("table_prefix") }}_products".ean13 as ean13 ,
    "{{ var("table_prefix") }}_products".height::float as height ,
    "{{ var("table_prefix") }}_products".width::float as width ,
    "{{ var("table_prefix") }}_products".location as location ,
    "{{ var("table_prefix") }}_products".manufacturer_name as manufacturer_name ,
    "{{ var("table_prefix") }}_products".unity as unity 
FROM "{{ var("table_prefix") }}_products"
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_products
ON _airbyte_raw_{{ var("table_prefix") }}_products._airbyte_ab_id = "{{ var("table_prefix") }}_products"._airbyte_ab_id
LEFT JOIN 
( SELECT 
    "_airbyte_ab_id", json_agg(json_build_object('url', '{{ var("prestashop_url") }}' || id || '-large_default/image.jpg')) AS images
from {{ var("table_prefix") }}_products_associations_images
	group by "_airbyte_ab_id" ) as image_table
on image_table._airbyte_ab_id = _airbyte_raw_{{ var("table_prefix") }}_products._airbyte_ab_id 
LEFT JOIN {{ var("table_prefix") }}_products_a__ions_stock_availables as pasa
ON pasa._airbyte_ab_id = "{{ var("table_prefix") }}_products"._airbyte_ab_id
LEFT JOIN {{ var("table_prefix") }}_stock_availables as stock
ON stock.id::text = pasa.id::text
WHERE pasa.id_product_attribute = '0'