{{ config(materialized='table') }}

WITH base_ports AS (
    SELECT
        world_port_index_number AS index_no,
        main_port_name,
        country_code,
        latitude,
        longitude,
        harbor_size,
        harbor_type,
        shelter_afforded AS shelter,
        -- Convert 'Y' to 1 for supplies
        CASE WHEN supplies_provisions = 'Y' THEN 1 ELSE 0 END AS has_provisions,
        CASE WHEN supplies_fuel_oil = 'Y' THEN 1 ELSE 0 END AS has_fuel_oil,
        CASE WHEN supplies_diesel_oil = 'Y' THEN 1 ELSE 0 END AS has_diesel,
        CASE WHEN supplies_potable_water = 'Y' THEN 1 ELSE 0 END AS has_water,
        CASE WHEN repairs = 'Y' THEN 1 ELSE 0 END AS has_repairs,
        
        -- Convert 'Y' to 1 for comms
        CASE WHEN communications_radio = 'Y' THEN 1 ELSE 0 END AS has_radio,
        CASE WHEN communications_telephone = 'Y' THEN 1 ELSE 0 END AS has_telephone,
        CASE WHEN communications_airport = 'Y' THEN 1 ELSE 0 END AS has_airport,
        CASE WHEN communications_telefax = 'Y' THEN 1 ELSE 0 END AS has_telefax
    FROM {{ source('silver', 'ports') }}
),
calculated_ports AS (
    SELECT 
        *,
        (has_provisions + has_fuel_oil + has_diesel + has_water + has_repairs) as supply_count,
        (has_radio + has_telephone + has_airport + has_telefax) as comm_count
    FROM base_ports
)

SELECT 
    index_no,
    main_port_name,
    country_code,
    latitude,
    longitude,
    harbor_size,
    harbor_type,
    shelter,
    CASE 
        WHEN supply_count = 5 THEN 'Excellent'
        WHEN supply_count >= 3 THEN 'Good'
        WHEN supply_count >= 1 THEN 'Limited'
        ELSE 'Unavailable'
    END AS supplies_rate,
    
    CASE 
        WHEN comm_count = 4 THEN 'Excellent'
        WHEN comm_count >= 2 THEN 'Good'
        WHEN comm_count >= 1 THEN 'Limited'
        ELSE 'Unavailable'
    END AS comm_rate
FROM calculated_ports
