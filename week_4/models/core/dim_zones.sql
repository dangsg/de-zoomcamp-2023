{{ config(materialized='table') }}

select * from {{ ref('taxi_zone_lookup') }}