from transitfeed import Poly
from transitfeed import Point


class TripUtil:
    def __init__(self, trip, vehicle_point, next_stop_id):
        self.trip = trip
        self.vehicle = vehicle_point
        self.next_stop_id = next_stop_id
        self.route = trip._schedule.GetRoute(trip.route_id)
        self.shape = trip._schedule.GetShape(trip.shape_id)
        poly = Poly()
        for pt in self.shape.points:
            poly.AddPoint(Point.FromLatLng(pt[0], pt[1]))
        self.poly_point = PolyWithPoint(poly, self.vehicle)
        stop_found = False
        for arr, dep, stop in self.trip.GetTimeStops():
            if stop.stop_id == self.next_stop_id:
                stop_found = True
                break
        if not self.next_stop_id:
            raise ValueError("Missing stop_id for trip_id {}".format(trip.trip_id))
        if not stop_found:
            raise ValueError("Stop_id {}, doesn't belong to trip_id {}".format(self.next_stop_id, trip.trip_id))

    def get_estimated_scheduled_time(self):
        """"Gets a time corresponding to current vehicle position for this trip. If for example our vehicle is between
            two stops, it should estimate it like stop1.departureTime + duration_betwen_stops * traveled_fraction"""
        prev = tuple()
        for arr, dep, stop in self.trip.GetTimeStops():
            if stop.stop_id == self.next_stop_id:
                if prev:
                    prev_dep = prev[0]
                    duration = arr - prev_dep
                    current_stop_point = Point.FromLatLng(stop.stop_lat, stop.stop_lon)
                    break
                else:#only for first stop
                    return arr
            prev = (dep, stop)

        prev_stop = prev[1]
        prev_stop_point = Point.FromLatLng(prev_stop.stop_lat, prev_stop.stop_lon)

        #this is sub-polyline between to stops
        sub_poly = get_sub_poly(self.poly_point.poly, prev_stop_point, current_stop_point)
        (pt_on_shape, i) = sub_poly.GetClosestPoint(self.vehicle)
        poly = PolyWithPoint(sub_poly, pt_on_shape)
        return prev_dep + duration * poly.get_current_segment_progress()


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
            raise PointOutOfPolylineException("The point {} is too far from shape".format(point))

    def get_distance_to_pt(self):
        length = 0
        for i in range(0, self.segment_indx):
            length += self.poly._points[i].GetDistanceMeters(self.poly._points[i + 1])
        return length + self.last_segment_progress

    def get_error(self):
        return self.error

    def get_current_segment_progress(self):
        return self.last_segment_progress / self.get_current_segment_length()

    def get_current_segment_length(self):
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