{{ config(materialized='table') }}

with
trips_unioned as (
    select * from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    -- where borough != 'Unknown'
)

select 
    trips_unioned.int64_field_0 as int64_field_0,
    trips_unioned.dispatching_base_num as dispatching_base_num,
    trips_unioned.pickup_datetime as pickup_datetime,
    trips_unioned.dropoff_datetime as dropoff_datetime,
    trips_unioned.PULocationID as PULocationID,
    trips_unioned.DOLocationID as DOLocationID,
    trips_unioned.SR_Flag as SR_Flag,
    trips_unioned.Affiliated_base_number as Affiliated_base_number,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.PULocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.DOLocationID = dropoff_zone.locationid