SELECT
    route_id,
    year,
    month,
    day,
    hour,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN delay_seconds < 30 THEN 1 ELSE 0 END) AS on_time_stops
FROM {{ ref('trip_updates_deduped') }}
GROUP BY ALL