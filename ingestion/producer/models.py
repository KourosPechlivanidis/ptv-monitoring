from typing import List, Optional
from pydantic import BaseModel

# Shared models
class Header(BaseModel):
    gtfs_realtime_version: str
    incrementality: int = None
    timestamp: int = None 

class VehicleDescriptor(BaseModel):
    vehicle_id: str
    label: str

# Vehicle position models
class VehiclePositionTrip(BaseModel):
    trip_id: str
    route_id: str
    start_time: str
    start_date: str

class Position(BaseModel):
    latitude: float
    longitude: float 

class Vehicle(BaseModel):
    trip: VehiclePositionTrip
    vehicle: VehicleDescriptor
    position: Position
    timestamp: int 

class VehiclePositionMessage(BaseModel):
    header: Header
    entity_id: str
    vehicle: Vehicle

# Trip update models
class TripUpdateTrip(BaseModel): 
    trip_id: str
    route_id: str
    start_time: str
    start_date: str
    schedule_relationship: int 

class Arrival(BaseModel):
    time: int

class Departure(BaseModel):
    time: int

class StopTimeUpdate(BaseModel):
    stop_sequence: int
    arrival: Optional[Arrival] 
    departure: Optional[Departure]
    stop_id: str
    schedule_relationship: int

class TripUpdate(BaseModel):
    trip: TripUpdateTrip
    stop_time_update: List[StopTimeUpdate]
    
class TripUpdateMessage(BaseModel):
    header: Header
    entity_id: str
    trip_update: TripUpdate
