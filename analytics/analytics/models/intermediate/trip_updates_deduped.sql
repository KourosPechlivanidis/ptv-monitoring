WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id, route_id, year, month, day, hour, stop_sequence
            ORDER BY source_timestamp
        ) AS row_number
    FROM delta_scan('s3://ptv-gtfs-silver/delta/trip_updates')
)
SELECT *
FROM ranked
WHERE row_number = 1