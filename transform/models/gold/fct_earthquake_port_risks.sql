{{ config(materialized='table') }}

WITH all_earthquakes AS (
    SELECT * 
    FROM {{ source('silver', 'earthquakes') }}
),
ports AS (
    SELECT * FROM {{ ref('dim_ports') }}
)

SELECT 
    e.unid AS earthquake_id,
    e.time AS earthquake_time,
    e.mag AS earthquake_magnitude,
    e.lat AS earthquake_lat,
    e.lon AS earthquake_lon,
    e.flynn_region,
    p.index_no AS port_id,
    p.main_port_name AS port_name,
    p.latitude AS port_lat,
    p.longitude AS port_lon,
    -- Trino great_circle_distance returns km
    great_circle_distance(e.lat, e.lon, p.latitude, p.longitude) AS distance_km,
    
    -- Port Risk Classification based on magnitude and distance
    CASE 
        WHEN great_circle_distance(e.lat, e.lon, p.latitude, p.longitude) <= 50 AND e.mag >= 5.0 THEN 'Dangerous'
        WHEN great_circle_distance(e.lat, e.lon, p.latitude, p.longitude) <= 100 AND e.mag >= 4.0 THEN 'Cautionary'
        ELSE 'Safe'
    END AS risk_classification

FROM all_earthquakes e
CROSS JOIN ports p
-- Widen the pre-filter to 2000km so BI tools can dynamically filter distance (REQ-03, REQ-05)
WHERE great_circle_distance(e.lat, e.lon, p.latitude, p.longitude) <= 2000
