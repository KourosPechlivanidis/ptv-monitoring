from abc import ABC, abstractmethod
from google.transit import gtfs_realtime_pb2
from models import Arrival, Departure, Header, Position, StopTimeUpdate, TripUpdate, TripUpdateMessage, VehiclePositionTrip, TripUpdateTrip, Vehicle, VehicleDescriptor, VehiclePositionMessage
from typing import List


class GTFSRealtimeParserABC(ABC):
    
    def __init__(self):
        pass
      

    @abstractmethod
    def parse(self) -> list[dict]:
        pass


class VehiclePositionParser(GTFSRealtimeParserABC):

    def parse(self, raw_bytes) -> List[Vehicle]:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        parsed = []
       
        # We are embedding the header in each message so each message is self-contained
        header = Header(
            gtfs_realtime_version=feed.header.gtfs_realtime_version,
            incrementality=feed.header.incrementality,
            timestamp=feed.header.timestamp 
        )

        for entity in feed.entity:
            
            if not entity.HasField("vehicle"):
                continue

            vp = entity.vehicle
            
            trip_data = VehiclePositionTrip(
                trip_id=vp.trip.trip_id,
                route_id=vp.trip.route_id,
                start_time=vp.trip.start_time,
                start_date=vp.trip.start_date,
            )

            vehicle_data = VehicleDescriptor(
                vehicle_id=vp.vehicle.id,
                label=vp.vehicle.label,
            )

            position_data = Position(
                latitude=vp.position.latitude,
                longitude=vp.position.longitude
            )

            vehicle_record = Vehicle(
                trip=trip_data,
                vehicle=vehicle_data,
                position=position_data,
                timestamp=vp.timestamp
            )

            vehicle_position_message = VehiclePositionMessage(
                header = header,
                entity_id=entity.id,
                vehicle=vehicle_record
            )
            
            parsed.append(vehicle_position_message.dict())

        return parsed



class TripUpdateParser(GTFSRealtimeParserABC):

    def parse(self, raw_bytes) -> List[dict]:
        
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        parsed = []

        # Embed the header into each parsed message
        header = Header(
            gtfs_realtime_version=feed.header.gtfs_realtime_version,
            incrementality=feed.header.incrementality,
            timestamp=feed.header.timestamp
        )

        for entity in feed.entity:

            if not entity.HasField("trip_update"):
                continue

            tu = entity.trip_update

            # Build Trip model
            trip_data = TripUpdateTrip(
                trip_id=tu.trip.trip_id,
                route_id=tu.trip.route_id,
                start_time=tu.trip.start_time,
                start_date=tu.trip.start_date,
                schedule_relationship=tu.trip.schedule_relationship
            )

            stop_time_updates = []
            for stu in tu.stop_time_update:

                arrival_data = None
                if stu.HasField("arrival"):
                    arrival_data = Arrival(time=stu.arrival.time)

                departure_data = None
                if stu.HasField("departure"):
                    departure_data = Departure(time=stu.departure.time)

                stu_record = StopTimeUpdate(
                    stop_sequence=stu.stop_sequence,
                    arrival=arrival_data,
                    departure=departure_data,
                    stop_id=stu.stop_id,
                    schedule_relationship=stu.schedule_relationship
                )

                stop_time_updates.append(stu_record)

            trip_update_record = TripUpdate(
                trip=trip_data,
                stop_time_update=stop_time_updates
            )

            message = TripUpdateMessage(
                header=header,
                entity_id=entity.id,
                trip_update=trip_update_record
            )

            parsed.append(message.dict())

        return parsed
