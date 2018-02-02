from transitfeed import Poly
from transitfeed import Point
from transitfeed import GetClosestPoint


class TripState:
    STOP_ERROR = 30
    VEHICLE_ERROR = 50

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

        self._stop_distances = []
        self._stop_times = trip.GetTimeStops()
        if not self._find_vehicle(last_known):
            raise PointOutOfPolylineException()

        if not self._scan_for_stops():
            raise StopFarFromPolylineException("Couldn't reach all stops for trip_id {}".format(self.trip.trip_id))

        self._find_previous_stop_indx()
        if self.next_stop_id and self.prev_stop_indx != -1 and not self._is_last_stop():
            next_stop_indx = [i for i in range(len(self._stop_times))
                              if self._stop_times[i][2].stop_id == self.next_stop_id][0]
            if not(next_stop_indx == self.prev_stop_indx + 1 or next_stop_indx == self.prev_stop_indx):
                raise AlgorithmErrorException("Vehicle localization error")
        self._calculate_distances()

    def get_distance_to_end_stop(self):
        end_stop = Point.FromLatLng(self._stop_times[-1][2].stop_lat, self._stop_times[-1][2].stop_lon)
        return self.vehicle.GetDistanceMeters(end_stop)

    def _is_last_stop(self):
        return self.prev_stop_indx == len(self._stop_times) - 1

    def _find_vehicle(self, start_pt_indx):
        self.segment_idx = None
        accum_distance = 0
        for i in range(start_pt_indx, len(self.poly.GetPoints()) - 1):
            pt_a, pt_b = self.poly.GetPoint(i), self.poly.GetPoint(i + 1)
            cur_segment_len = self.poly.distance[i]

            # improved = False
            dist_to_vehicle = pt_a.GetDistanceMeters(self.vehicle)
            error = self.VEHICLE_ERROR if self.segment_idx is None else self.error
            res = reach_to_point(self.vehicle, pt_a, pt_b, dist_to_vehicle, cur_segment_len, error)
            if res:
                self.segment_idx = i
                pt_on_shape = res[0]
                self.error = res[1]
                self.distance = accum_distance + pt_a.GetDistanceMeters(pt_on_shape)
                # improved = True

            # if self._is_vehicle_found() and not improved:
            #     break
            accum_distance += cur_segment_len
        return self.segment_idx is not None

    def _scan_for_stops(self):
        """"This is necessary, because shape_dist_traveled column is not reliable."""
        current_stop_index = 0
        accum_distance = 0
        for i in range(len(self.poly.GetPoints()) - 1):
            pt_a, pt_b = self.poly.GetPoint(i), self.poly.GetPoint(i + 1)
            cur_segment_len = self.poly.distance[i]

            while current_stop_index < len(self._stop_times):
                stop = Point.FromLatLng(self._stop_times[current_stop_index][2].stop_lat, self._stop_times[current_stop_index][2].stop_lon)
                dist_to_stop = pt_a.GetDistanceMeters(stop)
                res = reach_to_point(stop, pt_a, pt_b, dist_to_stop, cur_segment_len, self.STOP_ERROR)
                if res:
                    pt_on_shape = res[0]
                    self._stop_distances.append(accum_distance + pt_a.GetDistanceMeters(pt_on_shape))
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
        pt, i = self.poly.GetClosestPoint(self.vehicle)
        return pt.GetDistanceMeters(self.vehicle)

    def _is_vehicle_found(self):
        return self.segment_idx is not None

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
            two stops, it should estimate it like stop1.departureTime + duration_betwen_stops * traveled_fraction"""
        if self.prev_stop_indx == -1 or self._is_last_stop():
            return 0

        prev_dep = self._stop_times[self.prev_stop_indx][1]
        duration = self._stop_times[self.prev_stop_indx + 1][0] - prev_dep
        return prev_dep + duration * (self.dist_to_prev_stop / self.stop_interval)

    def get_stop_seq(self):
        return self.prev_stop_indx + 1


def reach_to_point(pt_x, pt_a, pt_b, a_x_len, a_b_len, allowed_error):
    """"Returns tuple(pt, error) if (pt_a, pt_b) segment approaches pt_x to at least allowed_error distance"""
    MAPPING_ERROR = 10
    if a_x_len < 50 or a_b_len > a_x_len - MAPPING_ERROR:  # proximity check
        pt = GetClosestPoint(pt_x, pt_a, pt_b)
        error = pt.GetDistanceMeters(pt_x)
        if error <= allowed_error:
            return (pt, error)
    return tuple()


class PointOutOfPolylineException(Exception):
    def __init__(self):
        super(PointOutOfPolylineException, self).__init__()


class StopFarFromPolylineException(Exception):
    def __init__(self, message):
        super(StopFarFromPolylineException, self).__init__(message)

class AlgorithmErrorException(Exception):
    def __init__(self, message):
        super(AlgorithmErrorException, self).__init__(message)