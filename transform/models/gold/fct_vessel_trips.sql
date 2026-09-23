{{ config(materialized='table') }}

SELECT 
    name AS vessel_name,
    type AS vessel_type,
    year_built,
    gross_tonnage,
    deadweight,
    length_m,
    beam_m,
    departure_date,
    last_port_name,
    last_port_country,
    arrival_date,
    destination_port_name,
    destination_port_country,
    reported_status,
    report_date,
    -- Delay analysis
    date_diff('hour', departure_date, arrival_date) AS trip_duration_hours,
    date_diff('hour', arrival_date, report_date) AS reporting_delay_hours
FROM {{ source('silver', 'vessels') }}
