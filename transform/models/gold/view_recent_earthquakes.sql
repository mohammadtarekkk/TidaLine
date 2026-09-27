{{ config(materialized='view') }}

SELECT * 
FROM {{ source('silver', 'earthquakes') }}
WHERE time > current_timestamp - INTERVAL '24' HOUR
