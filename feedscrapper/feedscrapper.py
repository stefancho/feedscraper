from google.transit import gtfs_realtime_pb2
from datetime import datetime
from dbmanager import DbManager
from transitfeed.shapelib import Point
from utils import TripUtil
from utils import PointOutOfPolylineException
import requests
import transitfeed
import time
import pytz
import argparse
import logging

logging.basicConfig(filename='feedscraper.log',level=logging.DEBUG)


def main(gtfs_zip_or_dir, feed_url, db_file, timezone, interval):
  loader = transitfeed.Loader(gtfs_zip_or_dir)
  schedule = loader.Load()
  db_manager = DbManager(db_file)

  if not schedule.GetShapeList():
    logging.error("This feed doesn't contain shape.txt file. Exit...")
    return

  logging.info("Start at local time {}".format(datetime.now()))
  while True:
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
          util = TripUtil(trip, vehiclePoint, entity.vehicle.stop_id)
        except PointOutOfPolylineException as e:
          logging.warning(e.message)
          continue
        except ValueError as ve:
          logging.warning(ve.message)
          continue

        logging.info("Vehicle position trip_id:{}, timestamp:{}, to_end:{}, stop_id:{}".format(
            entity.vehicle.trip.trip_id, entity.vehicle.timestamp, util.get_distance_to_end_stop(), entity.vehicle.stop_id
        ))

        cur_trip_progress = db_manager.get_latest_trip_progress(trip.trip_id, entity.vehicle.timestamp)
        new_progress = util.poly_point.get_overall_progress()
        if util.get_distance_to_end_stop() < 100 and cur_trip_progress == new_progress:
          continue
        if cur_trip_progress is not None and new_progress < cur_trip_progress:
          logging.warning("The trip_id {} seems to go backwards.".format(trip.trip_id))
          continue

        if not db_manager.has_log_duplicate(trip.trip_id, entity.vehicle.timestamp):
          estimated_time, stop_progress = util.get_estimated_scheduled_time()
          delay = calculate_delay(_normalize_time(entity.vehicle.timestamp, timezone), estimated_time)
          db_manager.insert_log(entity.vehicle.trip.route_id, trip.trip_id, util.get_stop_seq(),
                                entity.vehicle.timestamp, delay, new_progress, stop_progress)
    proc_time = time.time() - before
    time.sleep(interval - proc_time)


def _normalize_time(timestamp, timezone):
  """"Convert timestamp according to timezone, after that transform result to seconds after midnight"""
  time_zone = pytz.timezone(timezone)
  localized_time = datetime.fromtimestamp(float(timestamp), time_zone)
  return localized_time.hour * 3600 + localized_time.minute * 60 + localized_time.second


def calculate_delay(reporting_time, estimated_time):
  diff = reporting_time - estimated_time
  if abs(diff) > 23 * 3600:
    SEC_IN_DAY = 24 * 3600
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


if __name__ == "__main__":
  try:
    parser = argparse.ArgumentParser(description='This is gtfs realtime feed scraper.')
    parser.add_argument('--gtfsZipOrDir', help='Gtfs zip file or directory', required=True)
    parser.add_argument('--feedUrl', help='Gtfs realtime vehicle position url', required=True)
    parser.add_argument('--sqliteDb', help='A path to sqlite db file', required=True)
    parser.add_argument('--timezone', help='A timezone for this feed', required=True)
    parser.add_argument('--interval', help='A time interval between requests (in secs)', type=int, required=True)
    args = parser.parse_args()
    main(args.gtfsZipOrDir, args.feedUrl, args.sqliteDb, args.timezone, args.interval)
    # main("C:\\Users\\stefancho\\Desktop\\gtfs-burlington", "http://opendata.burlington.ca/gtfs-rt/GTFS_VehiclePositions.pb", "C:\\sqlite\\db\\burlington_v1.db", "US/Eastern", 50)
  except KeyboardInterrupt as err:
    logging.info("Ended at {}".format(datetime.now()))
