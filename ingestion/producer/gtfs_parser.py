from abc import ABC, abstractmethod
from google.transit import gtfs_realtime_pb2
from typing import List


class GTFSRealtimeParserABC(ABC):

    def __init__(self):
        pass


    @abstractmethod
    def parse(self) -> list[dict]:
        pass


class VehiclePositionParser(GTFSRealtimeParserABC):

    def parse(self, raw_bytes) -> List[dict]:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        parsed = []

        header = {
            "gtfs_realtime_version": feed.header.gtfs_realtime_version,
            "incrementality": feed.header.incrementality,
            "timestamp": feed.header.timestamp,
        }

        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue
            vp = entity.vehicle
            parsed.append({
                "header": header,
                "entity_id": entity.id,
                "vehicle": {
                    "trip": {
                        "trip_id": vp.trip.trip_id,
                        "route_id": vp.trip.route_id,
                        "start_time": vp.trip.start_time,
                        "start_date": vp.trip.start_date,
                    },
                    "vehicle": {
                        "vehicle_id": vp.vehicle.id,
                        "label": vp.vehicle.label,
                    },
                    "position": {
                        "latitude": vp.position.latitude,
                        "longitude": vp.position.longitude,
                    },
                    "timestamp": vp.timestamp,
                },
            })

        return parsed


class TripUpdateParser(GTFSRealtimeParserABC):

    def parse(self, raw_bytes) -> List[dict]:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        parsed = []

        header = {
            "gtfs_realtime_version": feed.header.gtfs_realtime_version,
            "incrementality": feed.header.incrementality,
            "timestamp": feed.header.timestamp,
        }

        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue
            tu = entity.trip_update

            stop_time_updates = []
            for stu in tu.stop_time_update:
                stop_time_updates.append({
                    "stop_sequence": stu.stop_sequence,
                    "arrival": {"time": stu.arrival.time} if stu.HasField("arrival") else None,
                    "departure": {"time": stu.departure.time} if stu.HasField("departure") else None,
                    "stop_id": stu.stop_id,
                    "schedule_relationship": stu.schedule_relationship,
                })

            parsed.append({
                "header": header,
                "entity_id": entity.id,
                "trip_update": {
                    "trip": {
                        "trip_id": tu.trip.trip_id,
                        "route_id": tu.trip.route_id,
                        "start_time": tu.trip.start_time,
                        "start_date": tu.trip.start_date,
                        "schedule_relationship": tu.trip.schedule_relationship,
                    },
                    "stop_time_update": stop_time_updates,
                },
            })

        return parsed
