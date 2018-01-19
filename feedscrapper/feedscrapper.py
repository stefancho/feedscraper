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


def main(gtfs_zip_or_dir, feed_url, db_file, timezone):
  loader = transitfeed.Loader(gtfs_zip_or_dir)
  schedule = loader.Load()
  db_manager = DbManager(db_file)

  if not schedule.GetShapeList():
    print "This feed doesn't contain shape.txt file. Exit..."
    return

  while True:
    feed = read_feed(feed_url)
    for entity in feed.entity:
      if entity.HasField('vehicle'):
        try:
          trip = schedule.GetTrip(entity.vehicle.trip.trip_id)
        except KeyError as e:
          print "Faulty trip_id for entity: {}".format(entity)
          continue

        vehiclePoint = Point.FromLatLng(entity.vehicle.position.latitude, entity.vehicle.position.longitude)
        try:
          util = TripUtil(trip, vehiclePoint, entity.vehicle.stop_id)
        except PointOutOfPolylineException as e:
          print "Shape too far from vehicle position for trip_id {}".format(trip.trip_id)
          continue
        except ValueError as ve:
          print ve.message
          continue

        estimated_time = util.get_estimated_scheduled_time()
        delay = calculate_delay(_normalize_time(entity.vehicle.timestamp, timezone), estimated_time)
        if db_manager.get_record_cont(trip.trip_id, entity.vehicle.timestamp) == 0:
          db_manager.insert_log((trip.trip_id, entity.vehicle.stop_id, entity.vehicle.timestamp, delay,
                                 len(util.poly_point.poly._points) - 1, util.poly_point.segment_indx,
                                 util.poly_point.get_current_segment_progress()))
    time.sleep(49)


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
    args = parser.parse_args()
    main(args.gtfsZipOrDir, args.feedUrl, args.sqliteDb, args.timezone)
  except KeyboardInterrupt as err:
    pass
