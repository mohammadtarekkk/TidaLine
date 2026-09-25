{{ config(materialized='table') }}

WITH recent_earthquakes AS (
    SELECT * 
    FROM {{ source('silver', 'earthquakes') }}
    WHERE time > current_timestamp - INTERVAL '24' HOUR
),
ports AS (
    SELECT * FROM {{ ref('dim_ports') }}
),
port_exposures AS (
    SELECT 
        p.index_no AS port_id,
        p.main_port_name,
        p.country_code,
        MAX(e.mag) AS max_magnitude_24h,
        MIN(great_circle_distance(e.lat, e.lon, p.latitude, p.longitude)) AS min_distance_km
    FROM ports p
    CROSS JOIN recent_earthquakes e
    GROUP BY 1, 2, 3
)
SELECT 
    port_id,
    main_port_name,
    country_code,
    max_magnitude_24h,
    min_distance_km,
    CASE 
        WHEN min_distance_km <= 50 AND max_magnitude_24h >= 5.0 THEN 'Dangerous'
        WHEN min_distance_km <= 100 AND max_magnitude_24h >= 4.0 THEN 'Cautionary'
        ELSE 'Safe'
    END AS current_risk_status
FROM port_exposures
