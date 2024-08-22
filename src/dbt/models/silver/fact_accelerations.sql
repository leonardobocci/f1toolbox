with

bronze_telemetry as (
    select *
    from {{ ref('fastf1_telemetry') }}
),

corner_profiles as (
    select
        *,
        case
            --threshold for a corner increases with speed (units in m/s^2)
            when
                speed between 0 and 100 and lateral_acceleration > 2
                then 'SLOW_CORNER'
            when
                speed between 100 and 170 and lateral_acceleration > 5
                then 'MEDIUM_CORNER'
            when speed > 170 and lateral_acceleration > 8 then 'FAST_CORNER'
            else 'STRAIGHT'
        end as corner_status
    from bronze_telemetry
),

braking_and_accelerations as (
    select
        *,
        case
            when
                longitudinal_acceleration < -1 and is_braking
                then 'BRAKING_ZONE'
            when
                longitudinal_acceleration > 1 and throttle_percentage > 95
                then 'ACCELERATION_ZONE'
            else 'LOW_LONGITUDINAL_LOAD'
        end as acceleration_status
    from corner_profiles
)

select * from braking_and_accelerations
