WITH hourly_stop_stats AS (
    SELECT
    stop_id,
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
 stats.*,
 stops.stop_name,
 FROM hourly_stop_stats as stats
LEFT JOIN (
    SELECT * FROM delta_scan('s3://ptv-gtfs-static/delta/stops')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stop_id ORDER BY ingested_at DESC) = 1
) AS stops
ON stats.stop_id = stops.stop_id