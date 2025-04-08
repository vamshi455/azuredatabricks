CREATE OR REPLACE FUNCTION vrr.InterpolatePVTCompletion (
    pressure FLOAT,
    completion VARCHAR(32),
    effective_date DATE
)
RETURNS TABLE (
    pressure FLOAT,
    oil_formation_volume_factor FLOAT,
    gas_formation_volume_factor FLOAT,
    water_formation_volume_factor FLOAT,
    solution_gas_oil_ratio FLOAT,
    volatized_oil_gas_ratio FLOAT,
    viscosity_oil FLOAT,
    viscosity_water FLOAT,
    viscosity_gas FLOAT,
    injected_gas_formation_volume_factor FLOAT,
    injected_water_formation_volume_factor FLOAT
)
AS
$$
    WITH pvt_with_end_date AS (
        SELECT 
            id_completion,
            test_date,
            pressure as pvt_pressure,
            oil_formation_volume_factor,
            gas_formation_volume_factor,
            water_formation_volume_factor,
            solution_gas_oil_ratio,
            volatized_oil_gas_ratio,
            viscosity_oil,
            viscosity_water,
            viscosity_gas,
            injected_gas_formation_volume_factor,
            injected_water_formation_volume_factor,
            COALESCE(
                (SELECT MIN(test_date) 
                 FROM vrr.completion_pvt_characteristics cpvt 
                 WHERE cpvt.test_date > cpc.test_date 
                 AND cpvt.id_completion = cpc.id_completion),
                '9999-12-31'::DATE
            ) as end_date
        FROM vrr.completion_pvt_characteristics cpc
        WHERE id_completion = completion
    ),
    exact_match AS (
        SELECT *
        FROM pvt_with_end_date
        WHERE pvt_pressure = pressure
        AND effective_date >= test_date
        AND effective_date < end_date
    ),
    lower_bound AS (
        SELECT *
        FROM pvt_with_end_date
        WHERE pvt_pressure = (
            SELECT MAX(pvt_pressure)
            FROM pvt_with_end_date p
            WHERE p.pvt_pressure < pressure
            AND effective_date >= p.test_date
            AND effective_date < p.end_date
        )
        AND effective_date >= test_date
        AND effective_date < end_date
    ),
    upper_bound AS (
        SELECT *
        FROM pvt_with_end_date
        WHERE pvt_pressure = (
            SELECT MIN(pvt_pressure)
            FROM pvt_with_end_date p
            WHERE p.pvt_pressure > pressure
            AND effective_date >= p.test_date
            AND effective_date < p.end_date
        )
        AND effective_date >= test_date
        AND effective_date < end_date
    ),
    second_lower_bound AS (
        SELECT *
        FROM pvt_with_end_date
        WHERE pvt_pressure = (
            SELECT MAX(pvt_pressure)
            FROM pvt_with_end_date p
            WHERE p.pvt_pressure < (SELECT pvt_pressure FROM lower_bound)
            AND effective_date >= p.test_date
            AND effective_date < p.end_date
        )
        AND effective_date >= test_date
        AND effective_date < end_date
    ),
    second_upper_bound AS (
        SELECT *
        FROM pvt_with_end_date
        WHERE pvt_pressure = (
            SELECT MIN(pvt_pressure)
            FROM pvt_with_end_date p
            WHERE p.pvt_pressure > (SELECT pvt_pressure FROM upper_bound)
            AND effective_date >= p.test_date
            AND effective_date < p.end_date
        )
        AND effective_date >= test_date
        AND effective_date < end_date
    ),
    interpolated_values AS (
        SELECT 
            pressure as result_pressure,
            CASE 
                -- Exact match
                WHEN EXISTS (SELECT 1 FROM exact_match) THEN (SELECT oil_formation_volume_factor FROM exact_match)
                -- Interpolation between upper and lower bounds
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM upper_bound) = 1 THEN
                    lb.oil_formation_volume_factor + 
                    ((pressure - lb.pvt_pressure) * (ub.oil_formation_volume_factor - lb.oil_formation_volume_factor)) / 
                    (ub.pvt_pressure - lb.pvt_pressure)
                -- Extrapolation above upper bound
                WHEN (SELECT COUNT(*) FROM upper_bound) = 1 AND (SELECT COUNT(*) FROM second_upper_bound) = 1 THEN
                    ub.oil_formation_volume_factor + 
                    ((pressure - ub.pvt_pressure) * (sub.oil_formation_volume_factor - ub.oil_formation_volume_factor)) / 
                    (sub.pvt_pressure - ub.pvt_pressure)
                -- Extrapolation below lower bound
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM second_lower_bound) = 1 THEN
                    lb.oil_formation_volume_factor + 
                    ((pressure - lb.pvt_pressure) * (slb.oil_formation_volume_factor - lb.oil_formation_volume_factor)) / 
                    (slb.pvt_pressure - lb.pvt_pressure)
                ELSE NULL
            END as oil_formation_volume_factor,
            CASE 
                WHEN EXISTS (SELECT 1 FROM exact_match) THEN (SELECT gas_formation_volume_factor FROM exact_match)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM upper_bound) = 1 THEN
                    lb.gas_formation_volume_factor + 
                    ((pressure - lb.pvt_pressure) * (ub.gas_formation_volume_factor - lb.gas_formation_volume_factor)) / 
                    (ub.pvt_pressure - lb.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM upper_bound) = 1 AND (SELECT COUNT(*) FROM second_upper_bound) = 1 THEN
                    ub.gas_formation_volume_factor + 
                    ((pressure - ub.pvt_pressure) * (sub.gas_formation_volume_factor - ub.gas_formation_volume_factor)) / 
                    (sub.pvt_pressure - ub.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM second_lower_bound) = 1 THEN
                    lb.gas_formation_volume_factor + 
                    ((pressure - lb.pvt_pressure) * (slb.gas_formation_volume_factor - lb.gas_formation_volume_factor)) / 
                    (slb.pvt_pressure - lb.pvt_pressure)
                ELSE NULL
            END as gas_formation_volume_factor,
            CASE 
                WHEN EXISTS (SELECT 1 FROM exact_match) THEN (SELECT water_formation_volume_factor FROM exact_match)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM upper_bound) = 1 THEN
                    lb.water_formation_volume_factor + 
                    ((pressure - lb.pvt_pressure) * (ub.water_formation_volume_factor - lb.water_formation_volume_factor)) / 
                    (ub.pvt_pressure - lb.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM upper_bound) = 1 AND (SELECT COUNT(*) FROM second_upper_bound) = 1 THEN
                    ub.water_formation_volume_factor + 
                    ((pressure - ub.pvt_pressure) * (sub.water_formation_volume_factor - ub.water_formation_volume_factor)) / 
                    (sub.pvt_pressure - ub.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM second_lower_bound) = 1 THEN
                    lb.water_formation_volume_factor + 
                    ((pressure - lb.pvt_pressure) * (slb.water_formation_volume_factor - lb.water_formation_volume_factor)) / 
                    (slb.pvt_pressure - lb.pvt_pressure)
                ELSE NULL
            END as water_formation_volume_factor,
            CASE 
                WHEN EXISTS (SELECT 1 FROM exact_match) THEN (SELECT solution_gas_oil_ratio FROM exact_match)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM upper_bound) = 1 THEN
                    lb.solution_gas_oil_ratio + 
                    ((pressure - lb.pvt_pressure) * (ub.solution_gas_oil_ratio - lb.solution_gas_oil_ratio)) / 
                    (ub.pvt_pressure - lb.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM upper_bound) = 1 AND (SELECT COUNT(*) FROM second_upper_bound) = 1 THEN
                    ub.solution_gas_oil_ratio + 
                    ((pressure - ub.pvt_pressure) * (sub.solution_gas_oil_ratio - ub.solution_gas_oil_ratio)) / 
                    (sub.pvt_pressure - ub.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM second_lower_bound) = 1 THEN
                    lb.solution_gas_oil_ratio + 
                    ((pressure - lb.pvt_pressure) * (slb.solution_gas_oil_ratio - lb.solution_gas_oil_ratio)) / 
                    (slb.pvt_pressure - lb.pvt_pressure)
                ELSE NULL
            END as solution_gas_oil_ratio,
            CASE 
                WHEN EXISTS (SELECT 1 FROM exact_match) THEN (SELECT volatized_oil_gas_ratio FROM exact_match)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM upper_bound) = 1 THEN
                    lb.volatized_oil_gas_ratio + 
                    ((pressure - lb.pvt_pressure) * (ub.volatized_oil_gas_ratio - lb.volatized_oil_gas_ratio)) / 
                    (ub.pvt_pressure - lb.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM upper_bound) = 1 AND (SELECT COUNT(*) FROM second_upper_bound) = 1 THEN
                    ub.volatized_oil_gas_ratio + 
                    ((pressure - ub.pvt_pressure) * (sub.volatized_oil_gas_ratio - ub.volatized_oil_gas_ratio)) / 
                    (sub.pvt_pressure - ub.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM second_lower_bound) = 1 THEN
                    lb.volatized_oil_gas_ratio + 
                    ((pressure - lb.pvt_pressure) * (slb.volatized_oil_gas_ratio - lb.volatized_oil_gas_ratio)) / 
                    (slb.pvt_pressure - lb.pvt_pressure)
                ELSE NULL
            END as volatized_oil_gas_ratio,
            CASE 
                WHEN EXISTS (SELECT 1 FROM exact_match) THEN (SELECT viscosity_oil FROM exact_match)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM upper_bound) = 1 THEN
                    lb.viscosity_oil + 
                    ((pressure - lb.pvt_pressure) * (ub.viscosity_oil - lb.viscosity_oil)) / 
                    (ub.pvt_pressure - lb.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM upper_bound) = 1 AND (SELECT COUNT(*) FROM second_upper_bound) = 1 THEN
                    ub.viscosity_oil + 
                    ((pressure - ub.pvt_pressure) * (sub.viscosity_oil - ub.viscosity_oil)) / 
                    (sub.pvt_pressure - ub.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM second_lower_bound) = 1 THEN
                    lb.viscosity_oil + 
                    ((pressure - lb.pvt_pressure) * (slb.viscosity_oil - lb.viscosity_oil)) / 
                    (slb.pvt_pressure - lb.pvt_pressure)
                ELSE NULL
            END as viscosity_oil,
            CASE 
                WHEN EXISTS (SELECT 1 FROM exact_match) THEN (SELECT viscosity_water FROM exact_match)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM upper_bound) = 1 THEN
                    lb.viscosity_water + 
                    ((pressure - lb.pvt_pressure) * (ub.viscosity_water - lb.viscosity_water)) / 
                    (ub.pvt_pressure - lb.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM upper_bound) = 1 AND (SELECT COUNT(*) FROM second_upper_bound) = 1 THEN
                    ub.viscosity_water + 
                    ((pressure - ub.pvt_pressure) * (sub.viscosity_water - ub.viscosity_water)) / 
                    (sub.pvt_pressure - ub.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM second_lower_bound) = 1 THEN
                    lb.viscosity_water + 
                    ((pressure - lb.pvt_pressure) * (slb.viscosity_water - lb.viscosity_water)) / 
                    (slb.pvt_pressure - lb.pvt_pressure)
                ELSE NULL
            END as viscosity_water,
            CASE 
                WHEN EXISTS (SELECT 1 FROM exact_match) THEN (SELECT viscosity_gas FROM exact_match)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM upper_bound) = 1 THEN
                    lb.viscosity_gas + 
                    ((pressure - lb.pvt_pressure) * (ub.viscosity_gas - lb.viscosity_gas)) / 
                    (ub.pvt_pressure - lb.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM upper_bound) = 1 AND (SELECT COUNT(*) FROM second_upper_bound) = 1 THEN
                    ub.viscosity_gas + 
                    ((pressure - ub.pvt_pressure) * (sub.viscosity_gas - ub.viscosity_gas)) / 
                    (sub.pvt_pressure - ub.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM second_lower_bound) = 1 THEN
                    lb.viscosity_gas + 
                    ((pressure - lb.pvt_pressure) * (slb.viscosity_gas - lb.viscosity_gas)) / 
                    (slb.pvt_pressure - lb.pvt_pressure)
                ELSE NULL
            END as viscosity_gas,
            CASE 
                WHEN EXISTS (SELECT 1 FROM exact_match) THEN (SELECT injected_gas_formation_volume_factor FROM exact_match)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM upper_bound) = 1 THEN
                    lb.injected_gas_formation_volume_factor + 
                    ((pressure - lb.pvt_pressure) * (ub.injected_gas_formation_volume_factor - lb.injected_gas_formation_volume_factor)) / 
                    (ub.pvt_pressure - lb.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM upper_bound) = 1 AND (SELECT COUNT(*) FROM second_upper_bound) = 1 THEN
                    ub.injected_gas_formation_volume_factor + 
                    ((pressure - ub.pvt_pressure) * (sub.injected_gas_formation_volume_factor - ub.injected_gas_formation_volume_factor)) / 
                    (sub.pvt_pressure - ub.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM second_lower_bound) = 1 THEN
                    lb.injected_gas_formation_volume_factor + 
                    ((pressure - lb.pvt_pressure) * (slb.injected_gas_formation_volume_factor - lb.injected_gas_formation_volume_factor)) / 
                    (slb.pvt_pressure - lb.pvt_pressure)
                ELSE NULL
            END as injected_gas_formation_volume_factor,
            CASE 
                WHEN EXISTS (SELECT 1 FROM exact_match) THEN (SELECT injected_water_formation_volume_factor FROM exact_match)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM upper_bound) = 1 THEN
                    lb.injected_water_formation_volume_factor + 
                    ((pressure - lb.pvt_pressure) * (ub.injected_water_formation_volume_factor - lb.injected_water_formation_volume_factor)) / 
                    (ub.pvt_pressure - lb.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM upper_bound) = 1 AND (SELECT COUNT(*) FROM second_upper_bound) = 1 THEN
                    ub.injected_water_formation_volume_factor + 
                    ((pressure - ub.pvt_pressure) * (sub.injected_water_formation_volume_factor - ub.injected_water_formation_volume_factor)) / 
                    (sub.pvt_pressure - ub.pvt_pressure)
                WHEN (SELECT COUNT(*) FROM lower_bound) = 1 AND (SELECT COUNT(*) FROM second_lower_bound) = 1 THEN
                    lb.injected_water_formation_volume_factor + 
                    ((pressure - lb.pvt_pressure) * (slb.injected_water_formation_volume_factor - lb.injected_water_formation_volume_factor)) / 
                    (slb.pvt_pressure - lb.pvt_pressure)
                ELSE NULL
            END as injected_water_formation_volume_factor
        FROM lower_bound lb
        FULL OUTER JOIN upper_bound ub ON 1=1
        FULL OUTER JOIN second_lower_bound slb ON 1=1
        FULL OUTER JOIN second_upper_bound sub ON 1=1
    )
    SELECT 
        ROUND(result_pressure, 5) as pressure,
        ROUND(oil_formation_volume_factor, 5) as oil_formation_volume_factor,
        ROUND(gas_formation_volume_factor, 5) as gas_formation_volume_factor,
        ROUND(water_formation_volume_factor, 5) as water_formation_volume_factor,
        ROUND(solution_gas_oil_ratio, 5) as solution_gas_oil_ratio,
        ROUND(volatized_oil_gas_ratio, 5) as volatized_oil_gas_ratio,
        ROUND(viscosity_oil, 5) as viscosity_oil,
        ROUND(viscosity_water, 5) as viscosity_water,
        ROUND(viscosity_gas, 5) as viscosity_gas,
        ROUND(injected_gas_formation_volume_factor, 5) as injected_gas_formation_volume_factor,
        ROUND(injected_water_formation_volume_factor, 5) as injected_water_formation_volume_factor
    FROM interpolated_values
    WHERE NOT (
        (SELECT COUNT(*) FROM pvt_with_end_date 
         WHERE effective_date >= test_date AND effective_date < end_date) = 0
        AND 
        (SELECT COUNT(*) FROM pvt_with_end_date 
         WHERE effective_date < test_date AND effective_date < end_date) = 0
    )
    LIMIT 1
$$;
