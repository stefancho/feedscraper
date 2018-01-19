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
            cur_feed[entity.vehicle.trip.trip_id] = entity.vehicle.timestamp
      if cur_feed != self.feed:
        self.feed = cur_feed
        if self.last_difference is not None:
            print now - self.last_difference
        self.last_difference = now


def test_feed(url):
    tester = FeedTester(url)
    stopFlag = threading.Event()
    thread = TimerThread(stopFlag, 50, tester.compare_feeds)
    thread.start()
    # thread.join()


if __name__ == "__main__":
    test_feed('http://opendata.burlington.ca/gtfs-rt/GTFS_VehiclePositions.pb')