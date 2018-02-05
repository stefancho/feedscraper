from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2
from datetime import datetime
from dbmanager import DbManager
from transitfeed.shapelib import Point
from utils import TripState, StopFarFromPolylineException, AlgorithmErrorException
from utils import VehicleOutOfPolylineException
import requests
import transitfeed
import time
import pytz
import argparse
import logging

SEC_IN_DAY = 24 * 3600
HOUR_SECS = 23 * 3600
time_zone = None


def main(gtfs_zip_or_dir, feed_url, db_file, interval):
  loader = transitfeed.Loader(feed_path=gtfs_zip_or_dir, memory_db=False)
  schedule = loader.Load()
  agency = schedule.GetAgencyList()[0]
  global time_zone
  time_zone = pytz.timezone(agency.agency_timezone)

  db_manager = DbManager(db_file)

  if not schedule.GetShapeList():
    logging.error("This feed doesn't contain shape.txt file. Exit...")
    return

  active_trips = ActiveTrips()

  logging.info("Start at local time {}".format(datetime.now()))
  while True:
    cnt = 0
    before = time.time()
    feed = read_feed(feed_url)
    for entity in feed.entity:
      if entity.HasField('vehicle'):
        trip_id = entity.vehicle.trip.trip_id
        try:
          trip = schedule.GetTrip(trip_id)
        except KeyError as e:
          logging.warning("Faulty trip_id for entity: {}".format(entity))
          continue

        vehiclePoint = Point.FromLatLng(entity.vehicle.position.latitude, entity.vehicle.position.longitude)
        try:
          trip_state = TripState(trip, vehiclePoint, entity.vehicle.stop_id)
        except VehicleOutOfPolylineException as e:
          logging.warning("Vehicle {1} is out of shape for trip_id {0}".format(trip_id, (entity.vehicle.position.latitude, entity.vehicle.position.longitude)))
          continue
        except StopFarFromPolylineException as e:
          logging.warning("Couldn't reach all stops for trip_id {}".format(trip_id))
          continue
        except AlgorithmErrorException as e:
          logging.warning("Vehicle localization error trip_id {}, vehicle {}".format(trip_id, (entity.vehicle.position.latitude, entity.vehicle.position.longitude)))
          continue

        cur_trip_progress = active_trips.get_trip_progress(trip_id)
        new_progress = trip_state.get_trip_progress()
        active_trips.add_update_trip(trip_id, entity.vehicle.timestamp, new_progress)
        if trip_state.get_distance_to_end_stop() < 100 and cur_trip_progress == new_progress:
          continue
        if cur_trip_progress is not None and new_progress < cur_trip_progress:
          logging.warning("The trip_id {} seems to go backwards.".format(trip_id))
          continue

        if not db_manager.has_log_duplicate(trip_id, entity.vehicle.timestamp):
          cnt += 1
          estimated_time = trip_state.get_estimated_scheduled_time()
          stop_progress = trip_state.get_stop_progress()
          delay = calculate_delay(_normalize_time(entity.vehicle.timestamp), estimated_time)
          start_day = active_trips.get_day_for_trip(trip_id)
          db_manager.insert_log(entity.vehicle.trip.route_id, trip_id, trip_state.get_stop_seq(),
                                entity.vehicle.timestamp, start_day, delay, new_progress, stop_progress)
    db_manager.commit()

    active_trips.clean_inactive_trips()
    proc_time = time.time() - before
    logging.info("Procesing time {}, records {}".format(proc_time, cnt))
    if interval - proc_time > 0:
      time.sleep(interval - proc_time)
    else:
      logging.warning("Processing is taking too long")


def _normalize_time(timestamp):
  localized_time = datetime.fromtimestamp(float(timestamp), time_zone)
  return localized_time.hour * 3600 + localized_time.minute * 60 + localized_time.second


def calculate_delay(reporting_time, estimated_time):
  diff = reporting_time - estimated_time
  if abs(diff) > HOUR_SECS:
    if diff > 0:
      return diff - SEC_IN_DAY
    else:
      return diff + SEC_IN_DAY
  return diff


def read_feed(url):
  feed = gtfs_realtime_pb2.FeedMessage()
  response = requests.get(url)
  try:
    feed.ParseFromString(response.content)
  except DecodeError as e:
    logging.error("Error while parsing protobuf input. {}".format(e.message))
  return feed

class ActiveTrips:
  def __init__(self):
    self.active_trips = {}

  def is_trip_active(self, trip_id):
    return self.active_trips.has_key(trip_id)

  def get_trip_progress(self, trip_id):
    if self.is_trip_active(trip_id):
      return self.active_trips.get(trip_id)[0]
    else:
      return None

  def get_day_for_trip(self, trip_id):
    if self.is_trip_active(trip_id):
      return self.active_trips.get(trip_id)[2]
    else:
      return None

  def add_update_trip(self, trip_id, timestamp, progress):
    if self.is_trip_active(trip_id):
      day = self.active_trips[trip_id][2]
    else:
      localized_time = datetime.fromtimestamp(float(timestamp), time_zone)
      day = localized_time.timetuple().tm_yday
    self.active_trips[trip_id] = (progress, timestamp, day)

  def clean_inactive_trips(self):
    """"Unused trips are removed after half an hour"""
    now = time.time()
    for trip_id in self.active_trips.keys():
      if now - self.active_trips[trip_id][1] > 1800:
        del self.active_trips[trip_id]


if __name__ == "__main__":
  try:
    parser = argparse.ArgumentParser(description='This is gtfs realtime feed scraper.')
    parser.add_argument('--gtfsZipOrDir', help='Gtfs zip file or directory', required=True)
    parser.add_argument('--feedUrl', help='Gtfs realtime vehicle position url', required=True)
    parser.add_argument('--sqliteDb', help='A path to sqlite db file', required=True)
    parser.add_argument('--interval', help='A time interval between requests (in secs)', type=int, required=True)
    parser.add_argument('--logFile', help='A time interval between requests (in secs)', required=False)
    args = parser.parse_args()
    if args.logFile is not None:
      logging.basicConfig(filename=args.logFile, level=logging.DEBUG)
    main(args.gtfsZipOrDir, args.feedUrl, args.sqliteDb, args.interval)
  except KeyboardInterrupt as err:
    logging.info("Ended at {}".format(datetime.now()))