import streamlit as st
from pinotdb import connect
import pandas as pd
from datetime import datetime
import plotly.express as px
import pydeck as pdk
from shapely import wkt
import time
import concurrent.futures
import random
import geopy.distance
from pyproj import Geod
from operator import itemgetter
from itertools import groupby

import folium
from streamlit_folium import folium_static
from scripts.routes import richmond, crystal_palace
from datetime import timedelta, datetime
from confluent_kafka import Producer
from shapely.geometry import Point
import uuid
import json

geoid = Geod(ellps="WGS84")

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))

def json_serializer(obj):
    if isinstance(obj, (datetime, datetime.date)):
        return obj.strftime("%Y-%m-%d %T%Z")
    raise "Type %s not serializable" % type(obj)

def publish_point(producer, run_id, user_id, raw_time, timestamp, point, distance_so_far, course):
    # print(raw_time, timestamp, point, distance_so_far)
    
    row = {
        "runId": run_id,
        "eventId": str(uuid.uuid4()),
        "competitorId": user_id,
        "rawTime": raw_time,
        "timestamp": timestamp,
        "lat": point[1],
        "lon": point[0],
        "distance": distance_so_far,
        "course": course
    }

    payload = json.dumps(row, default=json_serializer, ensure_ascii=False).encode('utf-8')
    producer.produce(topic='parkrun', key=str(row['competitorId']), value=payload, callback=acked)



def generate_route(points, geo_fence, how_many_get_stopped, min_pause, max_pause, start, id, seconds_per_km):   
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

    route = []
    route.append({"id": id, "rawTime": 0, "timestamp": start, "point": point1, "distance": 0})

    distance_so_far = 0
    totalTimePaused = 0
    should_pause = random.random() <= how_many_get_stopped
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

        route.append({"id": id, "rawTime": idx+1+totalTimePaused, "timestamp": start + timedelta(seconds=idx+1+totalTimePaused), "point": point2, "distance": distance_so_far})
    return route

def gen_route(points, geo_fence, how_many_get_stopped, min_pause, max_pause, start, fastest, slowest):
    id = random.randint(1, 1_000_000)
    seconds_per_km = random.randint(fastest, slowest)    
    return generate_route(points, geo_fence, how_many_get_stopped, min_pause, max_pause, start, id, seconds_per_km)

def get_courses(conn):    
    curs = conn.cursor()
    curs.execute("""
    select shortName,
           St_AsText(courseMap) as map
    from courses 
    """)
    df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    all_courses = {
        pair[0]: list(zip(wkt.loads(pair[1]).exterior.xy[0], 
                          wkt.loads(pair[1]).exterior.xy[1])) 
                 if wkt.loads(pair[1]).type == 'Polygon'
                 else list(zip(wkt.loads(pair[1]).xy[0], 
                               wkt.loads(pair[1]).xy[1])) 
        for pair in df.values.tolist()
    }

    for k, v in all_courses.items():
        v.reverse()
    return all_courses

def page_new_race():
    conn = connect(host='localhost', port=8099, path='/query/sql', scheme='http')
    st.header("Create new race")
    start_time = datetime.now()

    st.write(f"Start Time: {start_time}")

    all_courses = get_courses(conn)
    course = st.selectbox('Select course', all_courses.keys())

    curs = conn.cursor()
    curs.execute("""
    select shortName, 
           St_AsText(courseMap) AS courseMap,
           St_AsText(geoFenceLocation) as geoFence
    from courses
    WHERE shortName = %(shortName)s
    """, {"shortName": course})
    df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    course = df["shortName"].values[0]        
    course_map_wkt = df["courseMap"].values[0]
    geo_fence_wkt = df["geoFence"].values[0]

    geo_fence = wkt.loads(geo_fence_wkt)

    points = wkt.loads(course_map_wkt)
    x, y = points.exterior.coords.xy if points.type == 'Polygon' else points.xy

    m = folium.Map()

    loc = [(point[1], point[0]) for point in zip(x, y)]
    lat = sum([point[0] for point in loc]) / len(loc)
    lon = sum([point[1] for point in loc]) / len(loc)
    route = folium.PolyLine(loc, color='black', weight=2, opacity=0.8).add_to(m)

    m.fit_bounds(route.get_bounds())

    folium_static(m, width=350, height=200)

    competitors = st.number_input('Number of competitors', value=100)
    fastest = st.number_input('Fastest pace (seconds per km)', value=144)
    slowest = st.number_input('Slowest pace (seconds per km)', value=500)
    
    col1, col2, col3 = st.columns(3)    
    how_many_get_stopped = col1.number_input('Ratio that stop in the geo fence', value=0.1, step=0.01)
    min_pause = col2.number_input('Minimum time (in seconds) to pause', value=10, step=1)
    max_pause = col3.number_input('Maximum time (in seconds) to pause', value=60, step=1)

    ingestion_wait_period = st.number_input('Time (in seconds) to wait between ingesting batches of events', value=0.1, step=0.01)
    
    if not "race_in_progress" in st.session_state:
        st.session_state.race_in_progress= False

    def generate_race():
        st.session_state.race_in_progress = True    

    st.session_state.data = [] if 'data' not in st.session_state else st.session_state.data
    st.session_state.iter = 0 if 'iter' not in st.session_state else st.session_state.iter
    producer = Producer({'bootstrap.servers': 'localhost:9092'})    

    paused = False
    if st.session_state.iter == 0 and len(st.session_state.data) == 0:
        if st.button('Generate race', key="generateRace", on_click=generate_race):
            with st.spinner('Generating race...'):                                                                                                                                                                                                                                                                                                                                          
                points = all_courses[course]   
                entries = []
                with concurrent.futures.ProcessPoolExecutor() as executor:
                    start = time.perf_counter()
                    secs = range(0, int(competitors))
                    pool = [executor.submit(gen_route, points, geo_fence, how_many_get_stopped, min_pause, max_pause, start_time, fastest, slowest) for i in secs]
                    for i in concurrent.futures.as_completed(pool):        
                        entries.extend(i.result())
                    end = time.perf_counter()
            st.write(f'Race generated in {round(end-start, 2)} second(s)') 
            run_id = str(uuid.uuid4())

            row = {
                "runId": run_id,
                "course": course,
                "startTime": start_time
            }

            payload = json.dumps(row, default=json_serializer, ensure_ascii=False).encode('utf-8')
            producer.produce(topic='races', key=str(row['runId']), value=payload, callback=acked)
            producer.flush()
            
            entries.sort(key = itemgetter("rawTime"))
            groups = groupby(entries, itemgetter("rawTime"))
            
            for (key, data) in groups:
                for entry in data:
                    st.session_state.data.append({**entry, "key": key, "course": course, "run_id": run_id})
            st.experimental_rerun()
    else:
        st.button('Generate race', key="generateRace", disabled=True)        
        paused = st.checkbox("Pause Race", key="pauseRace")

    my_table = st.empty()
    df = pd.DataFrame([])
    for entry in st.session_state.data[st.session_state.iter:]:
        if paused:
            break        

        if entry["key"] != st.session_state.data[st.session_state.iter-1]["key"]:
            producer.flush()
            df = pd.DataFrame([{
                "id": entry["id"],
                "rawTime": entry["rawTime"],
                "distance": entry["distance"],
                "timestamp": entry["timestamp"],
                "lat": entry["point"][1],
                "lon": entry["point"][0]   
            }])
            time.sleep(ingestion_wait_period)

        with my_table.container():  
            st.write(df.tail(5))

        publish_point(producer, 
            entry["run_id"], entry["id"], entry["rawTime"], entry["timestamp"], 
            entry["point"], entry["distance"], entry["course"])            

        st.session_state.iter += 1

    producer.flush()
    if not paused and len(st.session_state.data) > 0:
        st.write("All events published")
        st.session_state.iter = 0
        st.session_state.data= []
        st.experimental_rerun()

st.title("Park Run Simulator")
now = datetime.now()
dt_string = now.strftime("%d %B %Y %H:%M:%S")

page_new_race()
