from transitfeed import Poly
from transitfeed import Point
from transitfeed import GetClosestPoint


class TripState:
    STOP_ERROR = 30
    VEHICLE_ERROR = 100

    def __init__(self, trip, vehicle_point, next_stop_id, last_known = 0):
        self.trip = trip
        self.vehicle = vehicle_point
        self.next_stop_id = next_stop_id
        self.calculated_length = None
        self.shape = trip._schedule.GetShape(trip.shape_id)
        self.poly = Poly()
        for pt in self.shape.points:
            self.poly.AddPoint(Point.FromLatLng(pt[0], pt[1]))
        self.poly.distance = [self.poly.GetPoint(i-1).GetDistanceMeters(self.poly.GetPoint(i))
                              for i in range(1, len(self.poly.GetPoints()))]

        self._stop_times = trip.GetTimeStops()
        self._stop_distances = [None for i in range(len(self._stop_times))]

        if not self._scan_for_stops(self.STOP_ERROR) and not self._scan_for_stops(self.STOP_ERROR*2):
            raise StopFarFromPolylineException()

        self.next_stop_idx = self._get_next_stop_idx()
        if not self._find_vehicle(last_known):
            raise VehicleOutOfPolylineException()

        self._find_previous_stop_indx()
        self._calculate_distances()

    def _get_next_stop_idx(self):
        if self.next_stop_id:
            for st_indx, st_time in enumerate(self._stop_times):
                if st_time[2].stop_id == self.next_stop_id:
                    return st_indx
        return None

    def _get_distance_range(self):
        if self.next_stop_idx is not None:
            if self.next_stop_idx == 0:
                return 0, self._stop_distances[0]
            else:
                return self._stop_distances[self.next_stop_idx - 1], self._stop_distances[self.next_stop_idx]
        return -0.1, float("inf")

    def get_distance_to_end_stop(self):
        end_stop = Point.FromLatLng(self._stop_times[-1][2].stop_lat, self._stop_times[-1][2].stop_lon)
        return self.vehicle.GetDistanceMeters(end_stop)

    def _is_last_stop(self):
        return self.prev_stop_indx == len(self._stop_times) - 1

    def _find_vehicle(self, start_pt_indx):
        min_distance, max_distance = self._get_distance_range()
        self.distance = None
        restricted_distance = None
        accum_distance = 0
        for i in range(start_pt_indx, len(self.poly.GetPoints()) - 1):
            pt_a, pt_b = self.poly.GetPoint(i), self.poly.GetPoint(i + 1)
            cur_segment_len = self.poly.distance[i]

            dist_to_vehicle = pt_a.GetDistanceMeters(self.vehicle)
            res = reach_to_point(self.vehicle, pt_a, pt_b, dist_to_vehicle, cur_segment_len, self.VEHICLE_ERROR)
            if res:
                pt_on_shape = res[0]
                distance = accum_distance + pt_a.GetDistanceMeters(pt_on_shape)
                if self.distance is None or self.distance is not None and self.error > res[1]:
                    self.distance = distance
                    self.error = res[1]
                if min_distance < distance <= max_distance and (restricted_distance is None or restricted_distance is not None and restricted_dist_err > res[1]):
                    restricted_distance = distance
                    restricted_dist_err = res[1]
            accum_distance += cur_segment_len

        if restricted_distance is not None:
            self.distance = restricted_distance
            self.error = restricted_dist_err

        return self.distance is not None

    def _scan_for_stops(self, STOP_ERROR):
        """"This is necessary, because shape_dist_traveled column is not reliable."""
        current_stop_index = 0
        accum_distance = 0
        for i in range(len(self.poly.GetPoints()) - 1):
            pt_a, pt_b = self.poly.GetPoint(i), self.poly.GetPoint(i + 1)
            cur_segment_len = self.poly.distance[i]

            while current_stop_index < len(self._stop_times):
                stop = Point.FromLatLng(self._stop_times[current_stop_index][2].stop_lat, self._stop_times[current_stop_index][2].stop_lon)
                dist_to_stop = pt_a.GetDistanceMeters(stop)
                res = reach_to_point(stop, pt_a, pt_b, dist_to_stop, cur_segment_len, STOP_ERROR)
                if res:
                    pt_on_shape = res[0]
                    self._stop_distances[current_stop_index] = accum_distance + pt_a.GetDistanceMeters(pt_on_shape)
                    current_stop_index += 1
                else:
                    break
            if current_stop_index == len(self._stop_times):
                break
            accum_distance += cur_segment_len
        return current_stop_index == len(self._stop_times)

    def get_stop_progress(self):
        if self.prev_stop_indx == -1 or self._is_last_stop():
            return 0
        return self.dist_to_prev_stop / self.stop_interval

    def debug_error(self):
        pt, segment_indx = self.poly.GetClosestPoint(self.vehicle)
        distance = 0
        for i in range(0, segment_indx):
            distance += self.poly._points[i].GetDistanceMeters(self.poly._points[i + 1])
        distance += self.poly._points[segment_indx].GetDistanceMeters(pt)
        return pt.GetDistanceMeters(self.vehicle), distance

    def _is_vehicle_found(self):
        return self.distance is not None

    def get_trip_len(self):
        if self.calculated_length is None:
            self.calculated_length = sum(self.poly.distance)
        return self.calculated_length

    def get_trip_progress(self):
        if self._is_last_stop():
            return 1
        elif self.prev_stop_indx == -1:
            return 0
        else:
            distance = self.distance - self._stop_distances[0]
            shape_len = self.get_trip_len() - self._stop_distances[0]
            return distance / shape_len

    def _find_previous_stop_indx(self):
        if self.distance >= self._stop_distances[-1]:
            self.prev_stop_indx = len(self._stop_distances) - 1
        elif self.distance < self._stop_distances[0]:
            self.prev_stop_indx = -1

        for i in range(len(self._stop_distances) - 1):
            if self._stop_distances[i] <= self.distance < self._stop_distances[i+1]:
                self.prev_stop_indx = i
                break

    def _calculate_distances(self):
        if self.prev_stop_indx == -1:
            self.stop_interval = self.dist_to_prev_stop = None
        elif self._is_last_stop():
            self.stop_interval = self.dist_to_next_stop = None
        else:
            self.stop_interval = self._stop_distances[self.prev_stop_indx + 1] - self._stop_distances[self.prev_stop_indx]
            self.dist_to_next_stop = self._stop_distances[self.prev_stop_indx + 1] - self.distance
            self.dist_to_prev_stop = self.stop_interval - self.dist_to_next_stop

    def get_estimated_scheduled_time(self):
        """"Gets a time corresponding to current vehicle position for this trip. If for example our vehicle is between
            two stops, it should estimate it like stop1.departureTime + duration_betwen_stops * traveled_fraction.
            A time is measured in seconds from midnight"""
        if self.prev_stop_indx == -1:
            return 0

        if self._is_last_stop():
            return self._stop_times[-1][0]

        prev_dep = self._stop_times[self.prev_stop_indx][1]
        duration = self._stop_times[self.prev_stop_indx + 1][0] - prev_dep
        return prev_dep + duration * (self.dist_to_prev_stop / self.stop_interval)

    def get_prev_stop_seq(self):
        return self.prev_stop_indx + 1


def reach_to_point(pt_x, pt_a, pt_b, a_x_len, a_b_len, allowed_error):
    """"Returns tuple(pt, error) if (pt_a, pt_b) segment approaches pt_x to at least allowed_error distance"""
    MAPPING_ERROR = 10
    if a_x_len < 50 or a_b_len > a_x_len - MAPPING_ERROR:  # proximity check
        pt = GetClosestPoint(pt_x, pt_a, pt_b)
        error = pt.GetDistanceMeters(pt_x)
        if error <= allowed_error:
            return pt, error
    return tuple()


class VehicleOutOfPolylineException(Exception): pass

class StopFarFromPolylineException(Exception): pass

class AlgorithmErrorException(Exception): pass