import geopy.distance
import random
from pyproj import Geod
from shapely.geometry import Point
from datetime import timedelta, datetime
from collections import deque

geoid = Geod(ellps="WGS84")

class Competitor:
    def __init__(self, id:int, how_many_get_stopped:int) -> None:
        self.id = id
        self.route = deque()
        self.should_pause = random.random() <= how_many_get_stopped

    def generate_points(self, points:[float], min_pause:int, max_pause:int, geo_fence, seconds_per_km, start):
        metres_per_second = 1000 / seconds_per_km

        all_points = []
        for point1, point2 in zip(points, points[1:]):
            distance = geopy.distance.distance(
                (list(point1)[1], list(point1)[0]),  
                (list(point2)[1], list(point2)[0])
            )
            metres_per_second_adjusted = metres_per_second + random.randint(int(-metres_per_second * 0.2), int(metres_per_second * 0.2))
            n_extra_points = distance.meters / metres_per_second_adjusted
            
            if n_extra_points >= 1:
                extra_points = geoid.npts(point1[0], point1[1], point2[0], point2[1], n_extra_points)

                all_points.append(point1)
                all_points.extend(extra_points)

        all_points.append(points[-1])

        route = deque()
        route.append({"id": self.id, "rawTime": 0, "timestamp": start, "point": point1, "distance": 0})

        distance_so_far = 0
        totalTimePaused = 0
        should_pause = self.should_pause
        has_already_paused = False

        pairs_of_points = list(zip(all_points, all_points[1:]))
        points_in_geo = [point2 for point1, point2 in pairs_of_points if geo_fence.contains(Point(point2))]
        points_in_geo_count = len(points_in_geo)

        selected_point = points_in_geo[random.randint(0, points_in_geo_count-1)]

        for idx, (point1, point2) in enumerate(pairs_of_points):
            dist = geopy.distance.distance((list(point1)[1], list(point1)[0]), (list(point2)[1], list(point2)[0])).meters
            distance_so_far += dist

            if selected_point == point2:
                if should_pause and not has_already_paused:
                    seconds_to_pause = random.randint(min_pause, max_pause)
                    for pause in range(0, seconds_to_pause):
                        raw_time = idx+1+totalTimePaused+pause
                        route.append({"id": id, "rawTime": raw_time, "timestamp": start + timedelta(seconds=raw_time), "point": point2, "distance": distance_so_far})
                    totalTimePaused += seconds_to_pause
                    has_already_paused = True

            route.append({"id": self.id, "rawTime": idx+1+totalTimePaused, "timestamp": start + timedelta(seconds=idx+1+totalTimePaused), "point": point2, "distance": distance_so_far})
        
        self.route = route

    def next_point(self):
        if self.route[0]["timestamp"] < datetime.now():
            return self.route.popleft()
        return None

    def __str__(self):
        return f"Competitor(id={self.id})"

    def __repr__(self):
        return self.__str__()


class Race:
    def __init__(self, id:str, points:[float], course:str) -> None:
        self.id = id
        self.competitors = []
        self.points = points
        self.course = course

    def __str__(self):
        return f"Race(id={self.id})"

    def __repr__(self):
        return self.__str__()

    def add_competitor(self, competitor:Competitor):
        self.competitors.append(competitor)

