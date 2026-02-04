SELECT
  trip_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  pickup_zone_id,
  dropoff_zone_id,
  fare_amount,
  tip_amount,
  total_amount,
  payment_type,
  pickup_date,
  pickup_hour,
  trip_duration_min
FROM {{ source('warehouse', 'silver_trips') }}
