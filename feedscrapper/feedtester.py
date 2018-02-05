from feedscrapper import read_feed
import time
import threading
from timerthread import TimerThread


class FeedTester:
    def __init__(self, url):
        self.feed = {}
        self.url = url
        self.last_difference = None

    def compare_feeds(self):
      now = time.time()
      feed = read_feed(self.url)
      cur_feed = {}
      for entity in feed.entity:
          if entity.HasField('vehicle'):
            if entity.vehicle.trip.trip_id:
                cur_feed[entity.vehicle.trip.trip_id] = entity.vehicle.timestamp
            elif entity.vehicle.trip.route_id:
                cur_feed[entity.vehicle.trip.route_id] = entity.vehicle.timestamp

      if cur_feed != self.feed:
        self.feed = cur_feed
        if self.last_difference is not None:
            print now - self.last_difference
        self.last_difference = now


def test_feed(url):
    tester = FeedTester(url)
    stopFlag = threading.Event()
    thread = TimerThread(stopFlag, 1, tester.compare_feeds)
    thread.start()
    # thread.join()


if __name__ == "__main__":
    # test_feed('http://opendata.burlington.ca/gtfs-rt/GTFS_VehiclePositions.pb')
    # test_feed('https://data.texas.gov/download/eiei-9rpf/application%2Foctet-stream')
    # test_feed('http://gtfs.translink.ca/gtfsposition?apikey=YondBWFAfXGcwwy2VieH')
    # test_feed('http://gtfs.ovapi.nl/nl/vehiclePositions.pb')
    # test_feed('http://developer.go-metro.com/TMGTFSRealTimeWebService/vehicle/VehiclePositions.pb')
    # test_feed('http://realtime.cota.com/TMGTFSRealTimeWebService/Vehicle/VehiclePositions.pb')
    # test_feed('http://gtfs.bigbluebus.com/vehiclepositions.bin')
    # test_feed('http://transport.orgp.spb.ru/Portal/transport/internalapi/gtfs/realtime/vehicle') #Saint Petersburg