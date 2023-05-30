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
from twisted.internet import task, reactor
from model import Race, Competitor

import folium
from streamlit_folium import folium_static
from scripts.routes import richmond, crystal_palace
from datetime import timedelta, datetime
from confluent_kafka import Producer
from shapely.geometry import Point
import uuid
import json
import requests

geoid = Geod(ellps="WGS84")
races = {}

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
                 if wkt.loads(pair[1]).geom_type == 'Polygon'
                 else list(zip(wkt.loads(pair[1]).xy[0], 
                               wkt.loads(pair[1]).xy[1])) 
        for pair in df.values.tolist()
    }

    for k, v in all_courses.items():
        v.reverse()
    return all_courses

def emit_locations(producer):
    global races
    print("emit_locations", races)
    if 'races' in races:
        for race_id, race in races.items():
            for competitor in race.competitors:
                entry = competitor.next_point()
                if entry:
                    print(entry)
                    publish_point(producer, 
                        race_id, entry["id"], entry["rawTime"], entry["timestamp"], 
                        entry["point"], entry["distance"], race.course
                    )       
        producer.flush()

def ebLoopFailed(failure):
    """
    Called when loop execution failed.
    """
    print("ebLoopFailed")
    print(str(failure))
    # reactor.stop()

def cbLoopDone(result):
    """
    Called when loop was stopped with success.
    """
    print("cbLoopDone")
    print("Race finished.")
    # reactor.stop()

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
    x, y = points.exterior.coords.xy if points.geom_type == 'Polygon' else points.xy

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


    if not "race_in_progress" in st.session_state:
        st.session_state.race_in_progress= False

    def generate_race():
        st.session_state.race_in_progress = True    
        
    paused = False
    if st.button('Generate race', key="generateRace", on_click=generate_race):
        with st.spinner('Generating race...'):                                                                                                                                                                                                                                                                                                                                          
            points = all_courses[course]   
            start = time.perf_counter()
            
            run_id = str(uuid.uuid4())

            row = {
                "runId": run_id,
                "course": course,
                "startTime": start_time
            }

            payload = json.dumps(row, default=json_serializer, ensure_ascii=False).encode('utf-8')
            producer.produce(topic='races', key=str(row['runId']), value=payload, callback=acked)
            producer.flush()

            payload = {"run_id": run_id, "points": points, "course": course, "how_many_get_stopped": how_many_get_stopped, "competitors": competitors,
                "fastest": fastest, "slowest": slowest, "min_pause": min_pause, "max_pause": max_pause}
            response = requests.post("http://localhost:8080", data=json.dumps(payload))

            end = time.perf_counter()
            st.write(f'Race generated in {round(end-start, 2)} second(s)')
            races = st.session_state['races']
        

        st.write(st.session_state['races'])
        

    # else:
    #     st.button('Generate race', key="generateRace", disabled=True)        
    #     paused = st.checkbox("Pause Race", key="pauseRace")

st.title("Park Run Simulator")
now = datetime.now()
dt_string = now.strftime("%d %B %Y %H:%M:%S")

if 'races' not in st.session_state:
    print("resetting races...")
    st.session_state['races'] = races

producer = Producer({'bootstrap.servers': 'localhost:9092'})

# l = task.LoopingCall(emit_locations, producer)
# loopDeferred = l.start(1.0)
# loopDeferred.addErrback(ebLoopFailed)
# loopDeferred.addCallback(cbLoopDone)

# if not reactor.running:
#     reactor.run(installSignalHandlers=0)

page_new_race()
