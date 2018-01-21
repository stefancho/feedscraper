from transitfeed import Poly
from transitfeed import Point


class TripUtil:
    def __init__(self, trip, vehicle_point, next_stop_id):
        self.trip = trip
        self.vehicle = vehicle_point
        self.next_stop_id = next_stop_id
        shape = trip._schedule.GetShape(trip.shape_id)
        poly = Poly()
        for pt in shape.points:
            poly.AddPoint(Point.FromLatLng(pt[0], pt[1]))
        self.poly_point = PolyWithPoint(poly, self.vehicle)
        if not self.next_stop_id:
            raise ValueError("Missing stop_id for trip_id {}".format(trip.trip_id))
        self._stop_times = self.trip.GetTimeStops()
        self._stop_indexes = [i for i in range(len(self._stop_times)) if self._stop_times[i][2].stop_id == self.next_stop_id]
        if not self._stop_indexes:
            raise ValueError("Stop_id {}, doesn't belong to trip_id {}".format(self.next_stop_id, trip.trip_id))
        if len(self._stop_indexes) > 1:
            raise ValueError("Trip_id {} has more than one stop with stop_id {}", trip.trip_id, self.next_stop_id)

    def get_distance_to_end_stop(self):
        endStop = Point.FromLatLng(self._stop_times[-1][2].stop_lat, self._stop_times[-1][2].stop_lon)
        return self.vehicle.GetDistanceMeters(endStop)

    def get_estimated_scheduled_time(self):
        """"Gets a time corresponding to current vehicle position for this trip. If for example our vehicle is between
            two stops, it should estimate it like stop1.departureTime + duration_betwen_stops * traveled_fraction"""
        stop_indx = self._stop_indexes[0]
        if stop_indx == 0:
            return (self._stop_times[0][0], 1)
        else:
            prev_dep = self._stop_times[stop_indx - 1][1]
            prev_stop = self._stop_times[stop_indx - 1][2]
            prev_stop_coord = Point.FromLatLng(prev_stop.stop_lat, prev_stop.stop_lon)

            cur_stop = self._stop_times[stop_indx][2]
            current_stop_coord = Point.FromLatLng(cur_stop.stop_lat, cur_stop.stop_lon)
            duration = self._stop_times[stop_indx][0] - prev_dep

        #this is sub-polyline between to stops
        sub_poly = get_sub_poly(self.poly_point.poly, prev_stop_coord, current_stop_coord)
        (pt_on_shape, i) = sub_poly.GetClosestPoint(self.vehicle)
        poly = PolyWithPoint(sub_poly, pt_on_shape)
        return (prev_dep + duration * poly.get_current_segment_progress(), poly.get_overall_progress())

    def get_stop_seq(self):
        return self._stop_indexes[0] + 1


class PointOutOfPolylineException(Exception):
    def __init__(self, message):
        super(PointOutOfPolylineException, self).__init__(message)


class PolyWithPoint:
    def __init__(self, poly, point):
        self.poly = poly
        self.point = point
        self.point_on_shape, self.segment_indx = poly.GetClosestPoint(point)
        self.last_segment_progress = poly._points[self.segment_indx].GetDistanceMeters(self.point_on_shape)
        self.error = point.GetDistanceMeters(self.point_on_shape)
        if self.error > 100:
            raise PointOutOfPolylineException("The point {} is {}m away from shape".format(point, self.error))

    def get_distance_to_pt(self):
        length = 0
        for i in range(0, self.segment_indx):
            length += self.poly._points[i].GetDistanceMeters(self.poly._points[i + 1])
        return length + self.last_segment_progress

    def get_error(self):
        return self.error

    def get_overall_progress(self):
        return self.get_distance_to_pt() / self.poly.LengthMeters()

    def get_current_segment_progress(self):
        return self.last_segment_progress / self._get_current_segment_length()

    def _get_current_segment_length(self):
        """"Current segment is the one that point lie in"""
        return self.poly._points[self.segment_indx].GetDistanceMeters(self.poly._points[self.segment_indx + 1])

def get_sub_poly(poly, point1, point2):
    """"Finds a new shape out of existing polyline, it's a subset between point1 and point2 inclusive.
        Parameter points should lie on the polyline. Both points should lie on the poly"""
    (pt1, pt_1_segment_indx) = poly.GetClosestPoint(point1)
    (pt2, pt_2_segment_indx) = poly.GetClosestPoint(point2)
    # assert pt1 == point1 and pt2 == point2
    res = Poly()
    res.AddPoint(point1)
    for i in range(pt_1_segment_indx + 1, pt_2_segment_indx + 1):
        res.AddPoint(poly._points[i])
    res.AddPoint(point2)
    return res