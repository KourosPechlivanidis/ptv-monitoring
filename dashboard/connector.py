import os
import duckdb
import pandas as pd
import redis
from datetime import datetime, timedelta
import numpy as np

class DataConnector:
    DB_PATH = os.getenv("DUCKDB_PATH", "../analytics/analytics/dev.duckdb")
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

    @classmethod
    def _query(cls, sql, params):
        """Standardized helper to handle connections and parameters."""
        with duckdb.connect(DataConnector.DB_PATH) as con:
            if params:
                return con.execute(sql, params).df()
            return con.execute(sql).df()

    @staticmethod
    def get_otp_score(mode=None):
        sql = """
            SELECT 
                SUM(on_time_stops)::FLOAT / NULLIF(SUM(total_stops), 0) * 100 as otp 
            FROM gold_route_hourly_stats
        """
        params = []
        if mode:
            # First filter, so WHERE is correct
            sql += " WHERE mode = ?"
            params.append(mode)

        df = DataConnector._query(sql, params)
        return df['otp'].iloc[0] if not df.empty and pd.notnull(df['otp'].iloc[0]) else 0.0

    @staticmethod
    def get_problem_routes(mode=None):
        # Sum the components across all hours to get an accurate total OTP
        sql = """
            SELECT 
                route_short_name as name,
                mode,
                trip_headsign,
                SUM(on_time_stops) as on_time_stops,
                SUM(total_stops) as total_stops,
                (SUM(on_time_stops)::FLOAT / NULLIF(SUM(total_stops), 0)) * 100 as otp_pct
            FROM gold_route_hourly_stats
            WHERE 1=1
        """
        
        params = []
        if mode:
            sql += " AND mode = ?"
            params.append(mode)

        # Grouping by name and mode collapses the hourly duplicates
        sql += """ 
            GROUP BY name, mode, trip_headsign
            HAVING otp_pct > 10
            ORDER BY otp_pct ASC 
            LIMIT 10
        """
        
        df = DataConnector._query(sql, params)

        return [
            {
                "name": f"Route {row['name']} to {row['trip_headsign']}", 
                "mode": row['mode'], 
                "total_stops": row['total_stops'],
                "on_time_stops": row['on_time_stops'],
                "otp_pct": f"{round(row['otp_pct'], 1)}%"
            } 
            for _, row in df.iterrows()
        ]

    @staticmethod
    def get_most_popular_stops(mode=None):
        sql = """
            SELECT 
                stop_name as name,
                mode,
                COUNT(*) as visit_count
            FROM gold_stop_hourly_stats
        """
        
        params = []
        if mode:
            # No WHERE exists yet, so this is fine
            sql += " WHERE mode = ?"
            params.append(mode)

        # Important: GROUP BY must come AFTER the WHERE clause
        sql += """
            GROUP BY name, mode
            ORDER BY visit_count DESC
            LIMIT 10
        """
        
        df = DataConnector._query(sql, params)

        return [
            {
                "name": row['name'], 
                "mode": row['mode'], 
                "visit_count": f"{int(row['visit_count'])}"
            } 
            for _, row in df.iterrows()
        ]
    
    @staticmethod
    def get_live_locations(mode=None):
        r = redis.Redis(host=DataConnector.REDIS_HOST, port=DataConnector.REDIS_PORT, decode_responses=True)
        vehicles = []
        
        # SCAN for all vehicle trip keys
        # Using a pattern matches your Redis HGETALL structure
        for key in r.scan_iter("vehicle:trip:*"):
            data = r.hgetall(key)
            
            # Apply mode filter if selected in UI
            if mode and data.get("mode") != mode:
                continue
                
            vehicles.append({
                "trip_id": data.get("trip_id"),
                "route_name": data.get("route_name"),
                "headsign": data.get("headsign"),
                "lat": float(data.get("latitude", 0)),
                "lon": float(data.get("longitude", 0)),
                "mode": data.get("mode"),
                "timestamp": data.get("timestamp")
            })
        return pd.DataFrame(vehicles)
    

    @staticmethod
    def get_delay_trend():
        """Generates realistic mock delay data for the last 24 hours."""
        now = datetime.now()
        hours = [now - timedelta(hours=i) for i in range(24)]
        hours.reverse()
        
        delays = []
        for h in hours:
            hour_val = h.hour
            # Baseline delay 1-2 mins
            base = np.random.uniform(1, 2)
            # Morning Peak (8-9 AM)
            if 7 <= hour_val <= 9:
                base += np.random.uniform(4, 7)
            # Evening Peak (5-6 PM)
            elif 16 <= hour_val <= 18:
                base += np.random.uniform(5, 9)
            # Late night
            elif 0 <= hour_val <= 4:
                base *= 0.5
            delays.append(round(base, 2))
            
        return pd.DataFrame({"time": [h.strftime("%H:00") for h in hours], "delay": delays})