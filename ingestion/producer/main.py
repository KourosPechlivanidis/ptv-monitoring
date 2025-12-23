from kafka_publisher import KafkaPublisher
from gtfs_poller import GTFSPoller
from gtfs_parser import TripUpdateParser, VehiclePositionParser
import threading
import time

if __name__ == "__main__":

    publisher = KafkaPublisher()

    # According to API specs, tram updates are pushed every minute, and other vehicles every 5s
    tram_vehicle_positions = GTFSPoller(
        feed_name="vehicle_positions",
        mode="tram",
        url="https://api.opendata.transport.vic.gov.au/opendata/public-transport/gtfs/realtime/v1/tram/vehicle-positions",
        topic="vehicle_positions",
        interval_seconds=60,
        publisher=publisher,
        parser=VehiclePositionParser()
    )

    bus_vehicle_positions = GTFSPoller(
        feed_name="vehicle_positions",
        mode="bus",
        url="https://api.opendata.transport.vic.gov.au/opendata/public-transport/gtfs/realtime/v1/bus/vehicle-positions",
        topic="vehicle_positions",
        interval_seconds=5,
        publisher=publisher,
        parser=VehiclePositionParser()
    )

    metro_vehicle_positions = GTFSPoller(
        feed_name="vehicle_positions",
        mode="metro",
        url="https://api.opendata.transport.vic.gov.au/opendata/public-transport/gtfs/realtime/v1/metro/vehicle-positions",
        topic="vehicle_positions",
        interval_seconds=5,
        publisher=publisher,
        parser=VehiclePositionParser()
    )

    tram_trip_updates = GTFSPoller(
        feed_name="trip_updates",
        mode="tram",
        url="https://api.opendata.transport.vic.gov.au/opendata/public-transport/gtfs/realtime/v1/tram/trip-updates",
        topic="trip_updates",
        interval_seconds=60,
        publisher=publisher,
        parser=TripUpdateParser()
    )

    bus_trip_updates = GTFSPoller(
        feed_name="trip_updates",
        mode="bus",
        url="https://api.opendata.transport.vic.gov.au/opendata/public-transport/gtfs/realtime/v1/bus/trip-updates",
        topic="trip_updates",
        interval_seconds=5,
        publisher=publisher,
        parser=TripUpdateParser()
    )

    metro_trip_updates = GTFSPoller(
        feed_name="trip_updates",
        mode="metro",
        url="https://api.opendata.transport.vic.gov.au/opendata/public-transport/gtfs/realtime/v1/metro/trip-updates",
        topic="trip_updates",
        interval_seconds=5,
        publisher=publisher,
        parser=TripUpdateParser()
    )

    threads = [
        threading.Thread(target=tram_vehicle_positions.poll_loop, daemon=True),
        threading.Thread(target=bus_vehicle_positions.poll_loop, daemon=True),
        threading.Thread(target=metro_vehicle_positions.poll_loop, daemon=True),
        threading.Thread(target=tram_trip_updates.poll_loop, daemon=True),
        threading.Thread(target=bus_trip_updates.poll_loop, daemon=True),
        threading.Thread(target=metro_trip_updates.poll_loop, daemon=True),
    ]

    for t in threads:
        t.start()
        
    while True:
        time.sleep(60)

