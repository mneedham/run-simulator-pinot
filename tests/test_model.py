import unittest
from model import Competitor
from datetime import datetime
from shapely.geometry import Polygon

class DeterministicRandom:
    def random(self):
        return 0.5  # or any other value you like

class TestCompetitor(unittest.TestCase):
    def test_generate_points(self):
        competitor = Competitor(1, 0.5)
        points = [(51.4545, -0.1818), (51.4545, -0.1817), (51.4546, -0.1817)]
        min_pause = 10
        max_pause = 60
        
        geo_fence_coordinates = [
            (0.0, 0.0), 
            (0.0, 1.0), 
            (1.0, 1.0), 
            (1.0, 0.0),
            (0.0, 0.0)
        ]

        geo_fence = Polygon(geo_fence_coordinates)
        seconds_per_km = 144
        start = datetime(2020, 1, 1)

        competitor.generate_points(points, min_pause, max_pause, geo_fence, seconds_per_km, start)

        self.assertTrue(len(competitor.route) > 0)

        next_point = competitor.next_point()

        self.assertEqual(next_point["id"], 1)
        self.assertEqual(next_point["rawTime"], 0)
        self.assertEqual(next_point["timestamp"], datetime(2020, 1, 1, 0, 0, 0))
        self.assertEqual(next_point["point"], (51.4545, -0.1817))
        self.assertEqual(next_point["distance"], 0)

        next_point = competitor.next_point()

        self.assertEqual(next_point["id"], 1)
        self.assertEqual(next_point["rawTime"], 1)
        self.assertEqual(next_point["timestamp"], datetime(2020, 1, 1, 0, 0, 1))
        self.assertEqual(next_point["point"], (51.4545, -0.18175000000000144))
        self.assertEqual(next_point["distance"], 5.528714349709607)

    def test_generate_points_inside_geofence(self):
        competitor = Competitor(1, 1)
        
        # Define points all inside the geo fence.
        points = [(51.5072, -0.1656), (51.5076, -0.1757), (51.5060, -0.1800), (51.5098, -0.1625)]
        
        min_pause = 10
        max_pause = 10

        # Define the coordinates for the vertices of your geo-fence.
        geo_fence_coordinates = [
        (51.5111, -0.1857),  # Northwest corner
        (51.5111, -0.1594),  # Northeast corner
        (51.5028, -0.1594),  # Southeast corner
        (51.5028, -0.1857),  # Southwest corner
        (51.5111, -0.1857)   # Closing the Polygon
    ]

        # Create the Polygon object.
        geo_fence = Polygon(geo_fence_coordinates)
        seconds_per_km = 144
        start = datetime(2020, 1, 1)

        competitor.generate_points(points, min_pause, max_pause, geo_fence, seconds_per_km, start)

        for r in competitor.route:
            print(r)

        next_point = competitor.next_point()

        self.assertEqual(next_point["id"], 1)
        self.assertEqual(next_point["rawTime"], 0)
        self.assertEqual(next_point["timestamp"], datetime(2020, 1, 1, 0, 0, 0))
        self.assertEqual(next_point["point"], (51.506, -0.18))
        self.assertEqual(next_point["distance"], 0)
