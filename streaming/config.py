# config.py
import os
from dataclasses import dataclass, field
from typing import List

@dataclass(frozen=True)
class BaseConfig:
    
    """
    
    Infrastructure settings applikcable to both trip updates and vehicle positions

    """
    
    kafka_bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    redis_host: str = os.getenv("REDIS_HOST", "redis")
    redis_port: int = int(os.getenv("REDIS_PORT", "6379"))
    base_static_path: str = os.getenv("STATIC_DATA_PATH", "static")
    timezone: str = os.getenv("APP_TIMEZONE", "Australia/Melbourne")

    def _get_paths(self, folder: str) -> List[str]:
        return [f"{self.base_static_path}/{m}/{folder}" for m in ["bus", "metro", "tram"]]

@dataclass(frozen=True)
class VehicleConfig(BaseConfig):
    
    """
    
    Config specific to vehicle positions
    
    """
    topic: str = "vehicle_positions"
    schema_path: str = "schemas/vehicle_position.json"
    
    # Vehicle positions need access to routes and trips to get route names + headsigns
    @property
    def route_paths(self) -> List[str]: return self._get_paths("routes")
    
    @property
    def trip_paths(self) -> List[str]: return self._get_paths("trips")

@dataclass(frozen=True)
class TripUpdateConfig(BaseConfig):
    """
    
    Config specific to trip updates
    
    """
    topic: str = "trip_updates"
    schema_path: str = "schemas/trip_update.json"
    
    # Trip updates need access to stop times to compute delays
    @property
    def stop_times_paths(self) -> List[str]: return self._get_paths("stop_times")