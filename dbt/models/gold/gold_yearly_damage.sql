{{
    config(
        materialized="table"
    )
}}

with d as (
    select *
    from {{ ref("silver_noaa_storm_events") }}
),
formatted as (
    select
        state,
        substr(cast(begin_yearmonth as varchar), 1, 4) as event_year,
        damage_property,
        try_cast(
            regexp_replace(lower(coalesce(damage_property, '')), '[a-z]', '', 'g') as double
        ) as damage_number,
        lower(right(coalesce(damage_property, ''), 1)) as damage_unit
    from d
),
damage as (
    select
        *,
        case
            when damage_unit = 'h' then coalesce(damage_number, 0) * 100
            when damage_unit = 'k' then coalesce(damage_number, 0) * 1000
            when damage_unit = 'm' then coalesce(damage_number, 0) * 1000000
            when damage_unit = 'b' then coalesce(damage_number, 0) * 1000000000
            else coalesce(damage_number, 0)
        end as property_damage
    from formatted
)

select
    event_year,
    state,
    sum(property_damage) as total_property_damage
from damage
group by 1, 2
