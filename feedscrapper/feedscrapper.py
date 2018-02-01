from google.transit import gtfs_realtime_pb2
from datetime import datetime
from dbmanager import DbManager
from transitfeed.shapelib import Point
from utils import TripState
from utils import PointOutOfPolylineException
import requests
import transitfeed
import time
import pytz
import argparse
import logging

SEC_IN_DAY = 24 * 3600
HOUR_SECS = 23 * 3600

def main(gtfs_zip_or_dir, feed_url, db_file, timezone, interval):
  loader = transitfeed.Loader(feed_path=gtfs_zip_or_dir, memory_db=False)
  schedule = loader.Load()
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
        try:
          trip = schedule.GetTrip(entity.vehicle.trip.trip_id)
        except KeyError as e:
          logging.warning("Faulty trip_id for entity: {}".format(entity))
          continue

        vehiclePoint = Point.FromLatLng(entity.vehicle.position.latitude, entity.vehicle.position.longitude)
        try:
          trip_state = TripState(trip, vehiclePoint, entity.vehicle.stop_id)
        except PointOutOfPolylineException as e:
          logging.warning(e.message)
          continue
        except ValueError as ve:
          logging.warning(ve.message)
          continue

        logging.info("Vehicle position trip_id:{}, timestamp:{}, to_end:{}, stop_id:{}".format(
            entity.vehicle.trip.trip_id, entity.vehicle.timestamp, trip_state.get_distance_to_end_stop(), entity.vehicle.stop_id
        ))

        cur_trip_progress = active_trips.get_trip_progress(trip.trip_id)
        new_progress = trip_state.get_trip_progress()
        active_trips.add_update_trip(trip.trip_id, entity.vehicle.timestamp, new_progress)
        if trip_state.get_distance_to_end_stop() < 100 and cur_trip_progress == new_progress:
          continue
        if cur_trip_progress is not None and new_progress < cur_trip_progress:
          logging.warning("The trip_id {} seems to go backwards.".format(trip.trip_id))
          continue

        if not db_manager.has_log_duplicate(trip.trip_id, entity.vehicle.timestamp):
          cnt += 1
          estimated_time = trip_state.get_estimated_scheduled_time()
          stop_progress = trip_state.dist_to_prev_stop / trip_state.stop_interval
          delay = calculate_delay(_normalize_time(entity.vehicle.timestamp, timezone), estimated_time)
          db_manager.insert_log(entity.vehicle.trip.route_id, trip.trip_id, trip_state.get_stop_seq(),
                                entity.vehicle.timestamp, delay, new_progress, stop_progress)
    db_manager.commit()
    proc_time = time.time() - before
    print "Procesing time {}, records {}".format(proc_time, cnt)
    cnt = 0
    # time.sleep(interval - proc_time)
    # logging.info("Processed {} requests for {} secs".format(len(feed.entity)), proc_time)
    # active_trips.clean_unactive_trips()
    time.sleep(10)

def _normalize_time(timestamp, timezone):
  """"Convert timestamp according to timezone, after that transform result to seconds after midnight"""
  # https: // maps.googleapis.com / maps / api / timezone / json?location = 40.06021, -82.969955 & timestamp = 1516809140 & key = AIzaSyADDgMlAZ5hnWEWIyO16T6YRyv - tFkuSMM
  time_zone = pytz.timezone(timezone)
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
  feed.ParseFromString(response.content)
  return feed

class ActiveTrips:
  def __init__(self):
    self.active_trips = {}

  def is_trip_active(self, trip_id):
    self.active_trips.has_key(trip_id)

  def get_trip_progress(self, trip_id):
    if self.is_trip_active(trip_id):
      progress, timestamp = self.active_trips.get(trip_id)
      return progress
    else:
      return None

  def add_update_trip(self, trip_id, timestamp, progress):
    self.active_trips[trip_id] = (progress, timestamp)

  def clean_unactive_trips(self):
    now = time.time()
    for trip_id in self.active_trips.keys():
      progress, timestamp = self.active_trips[trip_id]
      if now - timestamp > 1800:
        del self.active_trips[trip_id]

if __name__ == "__main__":
  try:
    # parser = argparse.ArgumentParser(description='This is gtfs realtime feed scraper.')
    # parser.add_argument('--gtfsZipOrDir', help='Gtfs zip file or directory', required=True)
    # parser.add_argument('--feedUrl', help='Gtfs realtime vehicle position url', required=True)
    # parser.add_argument('--sqliteDb', help='A path to sqlite db file', required=True)
    # parser.add_argument('--timezone', help='A timezone for this feed', required=True)
    # parser.add_argument('--interval', help='A time interval between requests (in secs)', type=int, required=True)
    # parser.add_argument('--logFile', help='A time interval between requests (in secs)', required=False)
    # args = parser.parse_args()
    # if args.logFile is not None:
    #   logging.basicConfig(filename=args.logFile, level=logging.DEBUG)
    # main(args.gtfsZipOrDir, args.feedUrl, args.sqliteDb, args.timezone, args.interval)
    main("C:\\Users\\stefancho\\Desktop\\gtfs-feeds\\gtfs-burlington", "http://opendata.burlington.ca/gtfs-rt/GTFS_VehiclePositions.pb", "C:\\sqlite\\db\\burlington.db", "US/Eastern", 50)


    # logging.basicConfig(filename='vancouver.log', level=logging.DEBUG)
    # main("C:\\Users\\stefancho\\Desktop\\gtfs-feeds\\vancouver.zip",
    #      "http://gtfs.translink.ca/gtfsposition?apikey=YondBWFAfXGcwwy2VieH", "C:\\sqlite\\db\\vancouver.db",
    #      "Canada/Pacific", 50)


  except KeyboardInterrupt as err:
    logging.info("Ended at {}".format(datetime.now()))