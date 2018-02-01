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
                              for i in range(range(1, len(self.poly.GetPoints())))]

        # self.poly_point = PolyWithPoint(poly, self.vehicle)
        self._stop_times = [(st.arrival_secs, st.departure_secs, st.stop, st.shape_dist_traveled)
                            for st in trip.GetStopTimes()]
        self._stop_coord = [Point.FromLatLng(self._stop_times[i][2].stop_lat, self._stop_times[i][2].stop_lon)
                            for i in range(len(self._stop_times))]
        self._compute_deltas()
        self._find_vehicle(last_known)
        if not self._is_vehicle_found():
            raise PointOutOfPolylineException("The vehicle {} is out of shape".format(self.vehicle))

        self._normalize_dist_traveled()
        self._get_previous_stop_indx()
        if self.next_stop_id:
            list = [i for i in range(len(self._stop_times)) if self._stop_times[i][2].stop_id == self.next_stop_id]
            next_stop_indx = list[0]
            assert next_stop_indx == self.prev_stop_indx + 1 or next_stop_indx == self.prev_stop_indx
        self._calculate_distances()

    def get_distance_to_end_stop(self):
        end_stop = Point.FromLatLng(self._stop_times[-1][2].stop_lat, self._stop_times[-1][2].stop_lon)
        return self.vehicle.GetDistanceMeters(end_stop)

    def _is_last_stop(self):
        return self.prev_stop_indx == len(self._stop_times) - 1

    def _compute_deltas(self):
        start_stop = Point.FromLatLng(self._stop_times[0][2].stop_lat, self._stop_times[0][2].stop_lon)
        self._start_delta = start_stop.GetDistanceMeters(self.poly.GetPoint(0))
        end_stop = Point.FromLatLng(self._stop_times[-1][2].stop_lat, self._stop_times[-1][2].stop_lon)
        self._end_delta = end_stop.GetDistanceMeters(self.poly.GetPoint(-1))

    def _find_vehicle(self, start_pt_indx):
        self.segment_idx = None

        accum_distance = 0
        for i in range(start_pt_indx, len(self.poly.GetPoints()) - 1):
            pt_a, pt_b = self.poly.GetPoint(i), self.poly.GetPoint(i + 1)
            cur_segment_len = self.poly.distance[i]

            improved = False
            dist_to_vehicle = pt_a.GetDistanceMeters(self.vehicle)
            error = self.VEHICLE_ERROR if self.segment_idx is None else self.error
            res = reach_to_point(self.vehicle, pt_a, pt_b, dist_to_vehicle, cur_segment_len, error)
            if res:
                self.segment_idx = i
                pt_on_shape = res[0]
                self.error = res[1]
                self.distance = accum_distance + pt_a.GetDistanceMeters(pt_on_shape)
                improved = True

            if self._is_vehicle_found() and not improved:
                break
            accum_distance += cur_segment_len

    def _scan_for_stops(self):
        """"This is necessary, because shape_dist_traveled column is not reliable."""
        current_stop_index = 0
        accum_distance = 0
        for i in range(len(self.poly.GetPoints()) - 1):
            pt_a, pt_b = self.poly.GetPoint(i), self.poly.GetPoint(i + 1)
            cur_segment_len = self.poly.distance[i]

            while current_stop_index < len(self._stop_times):
                dist_to_stop = pt_a.GetDistanceMeters(self._stop_coord[current_stop_index])
                res = reach_to_point(self._stop_coord[current_stop_index],
                                     pt_a, pt_b, dist_to_stop, cur_segment_len, self.STOP_ERROR)
                if res:
                    pt_on_shape = res[0]
                    self._stop_times[current_stop_index][3] = accum_distance + pt_a.GetDistanceMeters(pt_on_shape)
                    current_stop_index += 1
                else:
                    break
            if current_stop_index == len(self._stop_times) - 1:
                break
            accum_distance += cur_segment_len

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
            distance = self.distance - self._start_delta
            shape_len = self.get_trip_len() - self._start_delta - self._end_delta
            return distance / shape_len

    def _get_previous_stop_indx(self):
        #distance traveled from first stop to current position
        distance = self.distance - self._start_delta
        if distance >= self._stop_times[-1][3]:
            self.prev_stop_indx = len(self._stop_times) - 1
        elif distance < 0:
            self.prev_stop_indx = -1

        for i in range(len(self._stop_times) - 1):
            if self._stop_times[i][3] <= distance < self._stop_times[i + 1][3]:
                self.prev_stop_indx = i
                break

    def _calculate_distances(self):
        if self.prev_stop_indx == -1:
            self.stop_interval = self.dist_to_prev_stop = None
        elif self._is_last_stop():
            self.stop_interval = self.dist_to_next_stop = None
        else:
            dist_to_prev_stop = self._stop_times[self.prev_stop_indx][3] if self.prev_stop_indx > 0 else 0
            self.stop_interval = self._stop_times[self.prev_stop_indx + 1][3] - dist_to_prev_stop
            self.dist_to_next_stop = self._stop_times[self.prev_stop_indx + 1][3] - (self.distance - self._start_delta)
            self.dist_to_prev_stop = self.stop_interval - self.dist_to_next_stop

    def _normalize_dist_traveled(self):
        shape_dist = self.get_trip_len()
        stop_times_dist = self._stop_times[-1][3]
        if shape_dist > stop_times_dist:
            shape_dist -= self._start_delta
            shape_dist -= self._end_delta
        diff = shape_dist - stop_times_dist
        if diff != 0:
            for i in range(1, len(self._stop_times)):
                coeff = (self.shape.distance[i] - self.shape.distance[i-1]) / shape_dist
                self._stop_times[i][3] += diff * coeff

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
    def __init__(self, message):
        super(PointOutOfPolylineException, self).__init__(message)