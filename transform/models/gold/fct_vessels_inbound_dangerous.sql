{{ config(materialized='table') }}

WITH trips AS (
    SELECT * FROM {{ ref('fct_vessel_trips') }}
),
port_risk AS (
    SELECT * FROM {{ ref('fct_port_risk_status') }}
),
vessels_to_ports AS (
    SELECT 
        t.*,
        pr.port_id,
        pr.current_risk_status
    FROM trips t
    LEFT JOIN port_risk pr
        ON UPPER(TRIM(t.destination_port_name)) = UPPER(TRIM(pr.main_port_name))
)
SELECT *
FROM vessels_to_ports
WHERE current_risk_status IN ('Dangerous', 'Cautionary')
