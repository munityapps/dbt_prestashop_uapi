{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}
SELECT
    md5('{{ var("integration_id") }}' || sup.id ) as id,
    ('{{ var("integration_id") }}')::uuid as integration_id,
    sup.id as external_id,
    sup.name as name
from {{ var("table_prefix") }}_suppliers sup