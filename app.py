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
from streamlit_folium import st_folium
from scripts.routes import richmond, crystal_palace
from datetime import timedelta, datetime
from confluent_kafka import Producer
import uuid
import json
import numpy as np

geoid = Geod(ellps="WGS84")

def page_home():
    conn = connect(host='localhost', port=8099, path='/query/sql', scheme='http')

    curs = conn.cursor()
    curs.execute("""
    select runId, course, ToDateTime(startTime, 'YYYY-MM-dd HH:mm:ss') AS startTime,
        lookup('courses','longName','shortName',course) as courseName
    from races
    order by startTime DESC
    """)
    df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
    COURSES_MAP = {pair[0]: f"{pair[3]} at {pair[2]}" for pair in df.values.tolist()}

    run_id = st.selectbox(
        'Select event:', df['runId'].astype(str),
        format_func=lambda x:COURSES_MAP[ x ]
    )

    course = df["course"].values[0]

    curs = conn.cursor()
    curs.execute("""
    select course, runId, ToDateTime(startTime, 'YYYY-MM-dd HH:mm:ss') AS startTime,
        lookup('courses','distance','shortName',course) as distance,
        lookup('courses','longName','shortName',course) as courseName,
        St_AsText(lookup('courses','courseMap','shortName',course)) as courseMap,
        St_AsText(lookup('courses','geoFenceLocation','shortName',course)) as geoFenceLocation,
        St_AsText(lookup('courses','startLocation','shortName',course)) as startLocation,
        St_AsText(lookup('courses','endLocation','shortName',course)) as endLocation
    from races
    WHERE runId = %(runId)s
    """, {"runId": run_id})
    df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    course = df["course"].values[0]
    distance = df["distance"].values[0]
    course_name = df["courseName"].values[0]
    course_map_wkt = df["courseMap"].values[0]
    geo_fence_wkt = df["geoFenceLocation"].values[0]
    start_wkt = df["startLocation"].values[0]
    end_wkt = df["endLocation"].values[0]

    points = wkt.loads(course_map_wkt)
    x, y = points.exterior.coords.xy if points.geom_type == 'Polygon' else points.xy

    points_geo = wkt.loads(geo_fence_wkt)
    x_geo, y_geo = points_geo.exterior.coords.xy if points_geo.geom_type == 'Polygon' else points_geo.xy
    

    st.markdown(f"""
    **Event**: {run_id}  
    **Course Name**: {course_name}  
    **Race Start Time**: {df[df["runId"] == run_id].startTime.values[0]}
    """)

    curs = conn.cursor()
    curs.execute("""
    select competitorId,
        distance AS distanceCovered,
        round(%(courseDistance)d - distance, 1) AS distanceToGo,
        ToDateTime(1000 / (distance / rawTime) * 1000, 'HH:mm:ss') AS pacePerKm,
        ToDateTime(rawTime * 1000, 'mm:ss') AS raceTime
    from parkrun
    WHERE runId = %(runId)s
    ORDER BY distanceToGo, rawTime
    limit 10
    """, {"courseDistance": distance, "runId": run_id})
    df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    st.header("Leaderboard")
    styler = df.style.hide(axis='index')
    st.write(styler.to_html(), unsafe_allow_html=True)

    final_point = f"POINT({x[0]} {y[0]})"

    curs = conn.cursor()

    query = """
    select competitorId,
        distance AS distanceCovered,
        round(%(courseDistance)d - distance, 1) AS distanceToGo,
        ToDateTime(1000 / (distance / rawTime) * 1000, 'mm:ss') AS pacePerKm,
        ToDateTime(rawTime * 1000, 'mm:ss') AS raceTime,
        lon, lat, %(finalPoint)s
    from parkrun
    WHERE location <> ST_GeogFromText(%(finalPoint)s) AND runId = %(runId)s
    ORDER BY distanceToGo, rawTime
    limit 1000
    """

    curs.execute(query, {"courseDistance": distance, "runId": run_id, "finalPoint": final_point})
    df_front = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    query = """
    select competitorId,
        distance AS distanceCovered,
        round(%(courseDistance)d - distance, 1) AS distanceToGo,
        ToDateTime(1000 / (distance / rawTime) * 1000, 'mm:ss') AS pacePerKm,
        ToDateTime(rawTime * 1000, 'mm:ss') AS raceTime,
        lon, lat
    from parkrun
    WHERE location <> ST_GeogFromText(%(finalPoint)s) AND runId = %(runId)s
    ORDER BY distanceCovered
    limit 100
    """

    curs.execute(query, {"courseDistance": distance, "runId": run_id, "finalPoint": final_point})
    df_back = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    st.header("Currently running")
    st.subheader("At the front")
    styler = df_front.head(5)[["competitorId", "distanceCovered", "distanceToGo", "pacePerKm", "raceTime"]].style.hide(axis='index')
    if df_front.shape[0] > 0:
        st.write(styler.to_html(), unsafe_allow_html=True)
    else:
        st.write("No competitors currently running")

    st.subheader("At the back")
    styler = df_back.head(5)[["competitorId", "distanceCovered", "distanceToGo", "pacePerKm", "raceTime"]].style.hide(axis='index')
    if df_back.shape[0] > 0:
        st.write(styler.to_html(), unsafe_allow_html=True)
    else:
        st.write("No competitors currently running")

    st.header("Where are they?")

    m = folium.Map()

    # Example values
    start_wkt = "POINT (-0.063253 51.41917000000001)"
    end_wkt = "POINT (-0.064283 51.419324)"
    x_geo = [-0.0651347637176514, -0.0643622875213623, -0.0632894039154053, -0.0638902187347412, -0.0645339488983154, -0.0656068325042725, -0.0676238536834717, -0.0686323642730713, -0.0679242610931396, -0.0672805309295654, -0.0651347637176514]
    y_geo = [51.41916166790023, 51.41886727626769, 51.41846583007674, 51.417997471730985, 51.41767630894881, 51.416900156242406, 51.41687339212095, 51.41715441461497, 51.41776998166006, 51.41878698731156, 51.41916166790023]

    x_start, y_start = wkt.loads(start_wkt).coords.xy
    x_end, y_end = wkt.loads(end_wkt).coords.xy

    folium.Marker(location=(y_start[0], x_start[0]), icon=folium.Icon(color="green", icon="flag"), popup="Start").add_to(m)
    folium.Marker(location=(y_end[0], x_end[0]), icon=folium.Icon(color="red", icon="flag"), popup="Finish").add_to(m)    

    loc = [(point[1], point[0]) for point in zip(x_geo, y_geo)]
    lat = sum([point[0] for point in loc]) / len(loc)
    lon = sum([point[1] for point in loc]) / len(loc)
    folium.PolyLine(loc, color='red', weight=2, opacity=0.8).add_to(m)

    loc = [(point[1], point[0]) for point in zip(x, y)]
    lat = sum([point[0] for point in loc]) / len(loc)
    lon = sum([point[1] for point in loc]) / len(loc)
    route = folium.PolyLine(loc, color='#808080', weight=2, opacity=0.8).add_to(m)

    m.fit_bounds(route.get_bounds())

    fg = folium.FeatureGroup(name="Competitors")
    
    for lat, lon in zip(df_front.lat.values, df_front.lon.values):
        fg.add_child(
            folium.CircleMarker(location=(lat, lon), radius=3, color='Fuchsia')
        )    

    st_data = st_folium(m, 
        feature_group_to_add=fg,
        height=400,
        width=700,
    )

    st.subheader("Who's in the Geo fence?")

    query = """
    select count(*)
    from parkrun
    WHERE runId = %(runId)s
    AND ST_Contains(
        toGeometry(lookup('courses','geoFenceLocation','shortName',course)),
        toGeometry(location)
        ) = 1
    """

    curs.execute(query, {"runId": run_id})
    df_geo_fence_count = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    query = """
    select competitorId,
        distance AS distanceCovered,
        round(%(courseDistance)d - distance, 1) AS distanceToGo,
        ToDateTime(1000 / (distance / rawTime) * 1000, 'mm:ss') AS pacePerKm,
        ToDateTime(rawTime * 1000, 'mm:ss') AS raceTime,
        lon, lat
    from parkrun
    WHERE runId = %(runId)s
    AND ST_Contains(
        toGeometry(lookup('courses','geoFenceLocation','shortName',course)),
        toGeometry(location)
        ) = 1
    ORDER BY distanceCovered
    limit 100    
    """

    curs.execute(query, {"courseDistance": distance, "runId": run_id})
    df_geo_fence = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    query = """
    select competitorId,
		   (max("timestamp") - min("timestamp")) / 1000 AS timeInGeoFence
    from parkrun
    WHERE runId = %(runId)s
    AND ST_Contains(
        toGeometry(lookup('courses','geoFenceLocation','shortName',course)),
        toGeometry(location)
        ) = 1
    GROUP BY competitorId
    LIMIT 100
    option(skipUpsert=true)
    """

    curs.execute(query, {"courseDistance": distance, "runId": run_id})
    df_geo_fence_time = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    df_geo_fence = df_geo_fence.merge(df_geo_fence_time, on='competitorId')

    columns = ["competitorId", "distanceCovered", "distanceToGo", "pacePerKm", "raceTime", "timeInGeoFence"]
    styler = df_geo_fence.sort_values(by='timeInGeoFence', ascending=False).head(10)[columns].style.hide(axis='index')
    if df_geo_fence.shape[0] > 0:
        st.write(f"Number of competitors: {df_geo_fence_count['count(*)'].values[0]}")
        st.write(styler.to_html(), unsafe_allow_html=True)
    else:
        st.write("No competitors currently in the geo fence")

    curs = conn.cursor()
    curs.execute("""
    select count(*)
    from parkrun
    WHERE runId = %(runId)s AND location = ST_GeogFromText(%(finalPoint)s)
    """, {"courseDistance": distance, "runId": run_id, "finalPoint": final_point})
    finished = pd.DataFrame(curs, columns=[item[0] for item in curs.description])["count(*)"].values

    curs.execute("""
    select count(*)
    from parkrun
    WHERE runId = %(runId)s AND location <> ST_GeogFromText(%(finalPoint)s)
    """, {"courseDistance": distance, "runId": run_id, "finalPoint": final_point})
    not_finished = pd.DataFrame(curs, columns=[item[0] for item in curs.description])["count(*)"].values


    # finished = df[df.finished == 'true']["count(*)"].values
    # not_finished = df[df.finished == 'false']["count(*)"].values

    finished_score = finished[0] if len(finished) > 0 else 0
    not_finished_score = not_finished[0] if len(not_finished) > 0 else 0
    
    st.header("Finished?")
    st.write(f"Finished: {finished_score}, Not Finished: {not_finished_score}")
    
    if finished_score + not_finished_score > 0:
        percentage_finished = 100.0 * finished_score / (finished_score + not_finished_score)

        st.write("""
        <style>
        .progress {
            width:500px;
            height:50px;
            border:1px solid rgba(66,133,244,1.00);
            position:relative;
        }
        .progress:after {
            content:'\A';
            position:absolute;
            background:rgba(66,133,244,1.00);
            top:0; bottom:0;
            left:0;
            width:""" + str(percentage_finished) + """61%;
        }
        </style>


        <div class="progress">
        </div>
        """, unsafe_allow_html=True)


    query = """
    select competitorId,
        distance AS distanceCovered,
        round(%(courseDistance)d - distance, 1) AS distanceToGo,
        ToDateTime(1000 / (distance / rawTime) * 1000, 'mm:ss') AS pacePerKm,
        ToDateTime(rawTime * 1000, 'mm:ss') AS raceTime,
        ToDateTime("timestamp", 'HH:mm:ss') AS finishedAt
    from parkrun
    WHERE location = ST_GeogFromText(%(finalPoint)s) AND runId = %(runId)s
    ORDER BY rawTime DESC
    limit 5
    """

    curs.execute(query, {"courseDistance": distance, "runId": run_id, "finalPoint": final_point})
    df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    st.header("Who just finished?")
    styler = df.head(5)[["competitorId", "pacePerKm", "raceTime", "finishedAt"]].style.hide(axis='index')
    if df.shape[0] > 0:
        st.write(styler.to_html(), unsafe_allow_html=True)
    else:
        st.write("No competitors have finished")

st.sidebar.title("Park Run Simulator")
now = datetime.now()
dt_string = now.strftime("%d %B %Y %H:%M:%S")
st.sidebar.write(f"Last update: {dt_string}")

if not "sleep_time" in st.session_state:
    st.session_state.sleep_time = 2

if not "auto_refresh" in st.session_state:
    st.session_state.auto_refresh = True

auto_refresh = st.sidebar.checkbox('Auto Refresh?', st.session_state.auto_refresh)

if auto_refresh:
    number = st.sidebar.number_input('Refresh rate in seconds', value=st.session_state.sleep_time)
    st.session_state.sleep_time = number

page_home()

if auto_refresh:
    time.sleep(number)
    st.experimental_rerun()