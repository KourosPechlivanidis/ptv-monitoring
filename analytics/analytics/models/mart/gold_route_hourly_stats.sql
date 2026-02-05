WITH hourly_route_stats AS (
    SELECT
        trips.route_id,
        tud.mode,
        tud.year,
        tud.month,
        tud.day,
        tud.hour,
        COUNT(*) AS total_stops,
        SUM(CASE WHEN tud.delay_seconds < 30 THEN 1 ELSE 0 END) AS on_time_stops
    FROM {{ ref('trip_updates_deduped') }} AS tud 
    LEFT JOIN (
        SELECT * FROM delta_scan('s3://ptv-gtfs-static/delta/trips')
        QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY ingested_at DESC) = 1
    ) as trips
    ON trips.trip_id = tud.trip_id
    GROUP BY ALL
)

SELECT 
 stats.*,
 routes.route_short_name,
 routes.route_long_name
 FROM hourly_route_stats as stats
LEFT JOIN (
    SELECT * FROM delta_scan('s3://ptv-gtfs-static/delta/routes')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY route_id ORDER BY ingested_at DESC) = 1
) AS routes
ON stats.route_id = routes.route_id
