{{
    config(
        materialized='incremental',
        unique_key=['trip_id', 'route_id', 'year', 'month', 'day', 'hour', 'stop_sequence'],
        on_schema_change='append_new_columns'
    )
}}

WITH ranked_updates AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trip_id, route_id, year, month, day, hour, stop_sequence
            ORDER BY source_timestamp DESC
        ) AS row_num
    FROM delta_scan('s3://ptv-gtfs-silver/delta/trip_updates')

    {% if is_incremental() %}
    WHERE source_timestamp > (SELECT MAX(source_timestamp) FROM {{ this }})
    {% endif %}
)

SELECT 
    * EXCLUDE (row_num)
FROM ranked_updates
WHERE row_num = 1