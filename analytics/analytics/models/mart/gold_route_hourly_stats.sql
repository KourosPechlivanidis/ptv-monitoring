

WITH static_routes AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY route_id 
            ORDER BY ingested_at DESC
        ) as latest_rank
    FROM delta_scan('s3://ptv-gtfs-static/delta/routes/')
),

latest_static_routes AS (
    SELECT * EXCLUDE (latest_rank)
    FROM static_routes
    WHERE latest_rank = 1
),

hourly_route_stats AS (
    SELECT
    route_id,
    mode,
    year,
    month,
    day,
    hour,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN delay_seconds < 30 THEN 1 ELSE 0 END) AS on_time_stops
FROM {{ ref('trip_updates_deduped') }}
GROUP BY ALL
)

SELECT 
    route_stats.*,
    static_routes.route_long_name,
    static_routes.route_short_name,
    static_routes.route_type
FROM hourly_route_stats AS route_stats
LEFT JOIN latest_static_routes AS static_routes
    ON route_stats.route_id = static_routes.route_id