import unittest
from transitfeed import Loader
from transitfeed import Point
from utils import TripState
from utils import StopFarFromPolylineException


class TripStateTester(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TripStateTester, self).__init__(*args, **kwargs)
        loader = Loader(feed_path="./sample-feed")
        self.schedule = loader.Load()
        self.trip = self.schedule.GetTrip('247284')
        self.stop_times = self.trip.GetTimeStops()

    def test_trip_state_at_first_stop(self):
        trip_state = TripState(self.trip, Point.FromLatLng(42.14530077994279, 24.800326824188232), '')
        self.assertEqual(0, trip_state.get_stop_progress())
        self.assertEqual(0, trip_state.get_trip_progress())
        self.assertEqual(1, trip_state.get_stop_seq())
        self.assertEqual(0, trip_state.get_estimated_scheduled_time())

    def test_trip_state_at_last_stop(self):
        trip_state = TripState(self.trip, Point.FromLatLng(42.13357424874254,24.79510188102722), '')
        self.assertEqual(0, trip_state.get_stop_progress())
        self.assertEqual(1, trip_state.get_trip_progress())
        self.assertEqual(5, trip_state.get_stop_seq())
        self.assertEqual(self.stop_times[-1][0], trip_state.get_estimated_scheduled_time())

    def test_trip_state_before_first_stop(self):
        trip_state = TripState(self.trip, Point.FromLatLng(42.14507245405113, 24.79940072944254), '')
        self.assertEqual(0, trip_state.get_stop_progress())
        self.assertEqual(0, trip_state.get_trip_progress())
        self.assertEqual(0, trip_state.get_stop_seq())
        self.assertEqual(0, trip_state.get_estimated_scheduled_time())

        trip_state = TripState(self.trip, Point.FromLatLng(42.14491993843798, 24.79993315462003), '')
        self.assertEqual(0, trip_state.get_stop_progress())
        self.assertEqual(0, trip_state.get_trip_progress())
        self.assertEqual(0, trip_state.get_stop_seq())
        self.assertEqual(0, trip_state.get_estimated_scheduled_time())

    def test_trip_state_after_last_stop(self):
        trip_state = TripState(self.trip, Point.FromLatLng(42.13256957053682, 24.79477347922245), '')
        self.assertEqual(0, trip_state.get_stop_progress())
        self.assertEqual(1, trip_state.get_trip_progress())
        self.assertEqual(5, trip_state.get_stop_seq())
        self.assertEqual(self.stop_times[-1][0], trip_state.get_estimated_scheduled_time())

        trip_state = TripState(self.trip, Point.FromLatLng(42.131219133974184,24.79318410158157), '')
        self.assertEqual(0, trip_state.get_stop_progress())
        self.assertEqual(1, trip_state.get_trip_progress())
        self.assertEqual(5, trip_state.get_stop_seq())
        self.assertEqual(self.stop_times[-1][0], trip_state.get_estimated_scheduled_time())

    def test_trip_state_in_between_stops(self):
        trip_state = TripState(self.trip, Point.FromLatLng(42.14446157431765, 24.80178254507307), '')
        self.assertEqual(1, trip_state.get_stop_seq())
        self.assertTrue(trip_state.get_estimated_scheduled_time() < self.stop_times[1][0])

        trip_state = TripState(self.trip, Point.FromLatLng(42.14178793873418, 24.79772549935979), '')
        self.assertEqual(2, trip_state.get_stop_seq())
        self.assertTrue(self.stop_times[1][1] < trip_state.get_estimated_scheduled_time() < self.stop_times[2][0])

        trip_state = TripState(self.trip, Point.FromLatLng(42.13851564680248, 24.79707830644129), '')
        self.assertEqual(3, trip_state.get_stop_seq())
        self.assertTrue(self.stop_times[2][1] < trip_state.get_estimated_scheduled_time() < self.stop_times[3][0])

        trip_state = TripState(self.trip, Point.FromLatLng(42.13432309381219, 24.7953958493531), '')

        self.assertEqual(4, trip_state.get_stop_seq())
        self.assertTrue(self.stop_times[3][1] < trip_state.get_estimated_scheduled_time() < self.stop_times[4][0])

    def test_trip_state_error(self):
        DELTA = 1e-3
        trip_state = TripState(self.trip, Point.FromLatLng(42.14446157431765, 24.80178254507307), '')
        self.assertTrue(abs(trip_state.debug_error() - trip_state.error) < DELTA)
        trip_state = TripState(self.trip, Point.FromLatLng(42.14178793873418, 24.79772549935979), '')
        self.assertTrue(abs(trip_state.debug_error() - trip_state.error) < DELTA)
        trip_state = TripState(self.trip, Point.FromLatLng(42.13851564680248, 24.79707830644129), '')
        self.assertTrue(abs(trip_state.debug_error() - trip_state.error) < DELTA)
        trip_state = TripState(self.trip, Point.FromLatLng(42.13432309381219, 24.7953958493531), '')
        self.assertTrue(abs(trip_state.debug_error() - trip_state.error) < DELTA)
        trip_state = TripState(self.trip, Point.FromLatLng(42.131219133974184,24.79318410158157), '')
        self.assertTrue(abs(trip_state.debug_error() - trip_state.error) < DELTA)
        trip_state = TripState(self.trip, Point.FromLatLng(42.14507245405113, 24.79940072944254), '')
        self.assertTrue(abs(trip_state.debug_error() - trip_state.error) < DELTA)

    def test_displaced_stop(self):
        self.trip = self.schedule.GetTrip('247285')
        try:
            trip_state = TripState(self.trip, Point.FromLatLng(42.14446157431765, 24.80178254507307), '')
        except StopFarFromPolylineException as e:
            self.fail("Stop finding doesn't work")



if __name__ == "__main__":
    unittest.main()
