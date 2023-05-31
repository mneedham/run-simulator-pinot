import unittest
from model import Competitor
from datetime import datetime
from shapely.geometry import Polygon

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
        first_point_in_geo = lambda points_in_geo: points_in_geo[0]
        competitor = Competitor(1, 1, pace_variance=0, geo_fence_selection_fn=first_point_in_geo)
        
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

        self.assertEqual(len(competitor.route), 532)

        next_point = competitor.next_point()

        self.assertEqual(next_point["id"], 1)
        self.assertEqual(next_point["rawTime"], 0)
        self.assertEqual(next_point["timestamp"], datetime(2020, 1, 1, 0, 0, 0))
        self.assertEqual(next_point["point"], (51.506, -0.18))
        self.assertEqual(next_point["distance"], 0)

        next_point = competitor.next_point()

        self.assertEqual(next_point["id"], 1)
        self.assertEqual(next_point["rawTime"], 1)
        self.assertEqual(next_point["timestamp"], datetime(2020, 1, 1, 0, 0, 1))
        self.assertEqual(next_point["point"], (51.507202484470774, -0.1656627329196057))
        self.assertEqual(next_point["distance"],  6.942159043009585)

        for i in range(0, 10):
            next_point = competitor.next_point()

        self.assertEqual(next_point["id"], 1)
        self.assertEqual(next_point["rawTime"], 11)
        self.assertEqual(next_point["timestamp"], datetime(2020, 1, 1, 0, 0, 11))
        self.assertEqual(next_point["point"], (51.507202484470774, -0.1656627329196057))
        self.assertEqual(next_point["distance"],  6.942159043009585)
      
        # Pausing is finished
        next_point = competitor.next_point()

        self.assertEqual(next_point["id"], 1)
        self.assertEqual(next_point["rawTime"], 12)
        self.assertEqual(next_point["timestamp"], datetime(2020, 1, 1, 0, 0, 12))
        self.assertEqual(next_point["point"], (51.507204968941565, -0.1657254658392072))
        self.assertEqual(next_point["distance"],  13.884318086017416)