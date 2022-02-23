from shapely.geometry import Polygon, LineString
from routes import richmond, crystal_palace, lyme_park, molevalley
import uuid

points = molevalley ()
points.reverse()
polygon_geom = LineString(points)

print(uuid.uuid4())
print(polygon_geom)