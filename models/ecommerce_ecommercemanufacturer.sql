{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}
SELECT
    md5('{{ var("integration_id") }}' || manu.id ) as id,
    ('{{ var("integration_id") }}')::uuid as integration_id,
    manu.id as external_id,
    manu.name as name
from {{ var("table_prefix") }}_manufacturers manu