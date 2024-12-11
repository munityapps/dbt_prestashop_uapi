{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT 
md5('{{ var("integration_id") }}' || comb.id || 'variation' || 'prestashop') as id,
comb.id as external_id,
comb.quantity::int as quantity_available,
comb.minimal_quantity::int as minimal_quantity,
comb.reference as reference,
comb.ean13 as ean13,
NULL::float as regular_price,
comb.wholesale_price::float as sale_price,
comb.price::float as price,
('{{ var("integration_id") }}')::uuid as integration_id,
md5('{{ var("integration_id") }}' || prod.id ||'product' ||'prestashop')  as product_id
FROM {{ var("table_prefix") }}_combinations comb
LEFT JOIN {{ var("table_prefix") }}_products prod
ON comb.id_product::int = prod.id::int