#!/usr/bin/env python

# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.

from twisted.web import server, resource
from twisted.internet import reactor, task, endpoints, threads, defer
from twisted.internet.protocol import Factory, Protocol
from confluent_kafka import Producer
import json
from model import Race, Competitor
from types import SimpleNamespace
from datetime import datetime
from pinotdb import connect
import pandas as pd
from shapely import wkt
import random
import uuid

races = {}
producer = Producer({'bootstrap.servers': 'localhost:9092'})
conn = connect(host='localhost', port=8099, path='/query/sql', scheme='http')

def get_geo_fence(course):
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

    return geo_fence


class Courses(resource.Resource):
    isLeaf = True

    def render_POST(self, request):
        body = request.content.read()
        
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            request.setResponseCode(400)  # Bad request
            return b"Bad request: body is not valid JSON."

        # Print the parsed JSON
        data_obj = SimpleNamespace(**data)
        print(data_obj)

        race = Race(id=data_obj.run_id, points=data_obj.points, course=data_obj.course)

        d_geo_fence = threads.deferToThread(get_geo_fence, data_obj.course)

        def on_error(failure):
            request.setResponseCode(500)
            request.write(b"Internal Server Error")
            request.finish()

        def on_geo_fence_ready(geo_fence):
            start_time = datetime.now()

            deferreds = []

            for idx in range(0, int(data_obj.competitors)):
                competitor_id = random.randint(1, 1_000_000)
                competitor = Competitor(id=competitor_id, how_many_get_stopped = data_obj.how_many_get_stopped)
                seconds_per_km = random.randint(data_obj.fastest, data_obj.slowest)
                
                deferred_job = threads.deferToThread(
                    competitor.generate_points,
                    race.points,
                    data_obj.min_pause,
                    data_obj.max_pause,
                    geo_fence,
                    seconds_per_km,
                    start_time
                )
                deferreds.append(deferred_job)
                deferred_job.addCallback(lambda _, competitor=competitor: race.add_competitor(competitor))

            def on_all_done(_):
                races[race.id] = race
                request.write(b"Success")
                request.finish()

            # Wait for all deferreds to complete before sending the response
            defer.gatherResults(deferreds).addCallbacks(on_all_done, on_error)

        d_geo_fence.addCallbacks(on_geo_fence_ready, on_error)

        return server.NOT_DONE_YET

def emit_events():
    messages_flushed = 0
    global races
    for race_id, race in races.items():
        for competitor in race.competitors:
            entry = competitor.next_point()
            if entry:
                publish_point(producer, 
                    race_id, entry["id"], entry["rawTime"], entry["timestamp"], 
                    entry["point"], entry["distance"], race.course
                )       
                messages_flushed +=1
        producer.flush()        
    if messages_flushed > 0:
        print(f"{datetime.now()}: Flushed {messages_flushed} events")

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

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))

def json_serializer(obj):
    if isinstance(obj, datetime):  # This refers to datetime.datetime because of your import
        return obj.strftime("%Y-%m-%d %T%Z")
    raise TypeError("Type %s not serializable" % type(obj))


def publish_point(producer, run_id, user_id, raw_time, timestamp, point, distance_so_far, course):
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

    try:
        payload = json.dumps(row, default=json_serializer, ensure_ascii=False).encode('utf-8')
        producer.produce(topic='parkrun', key=str(row['competitorId']), value=payload, callback=acked)
    except TypeError:
        print(f"Failed to parse: {row}")

def main():
    site = server.Site(Courses())
    endpoint = endpoints.TCP4ServerEndpoint(reactor, 8080)
    endpoint.listen(site)

    l = task.LoopingCall(emit_events)
    loopDeferred = l.start(1.0)
    loopDeferred.addErrback(ebLoopFailed)
    loopDeferred.addCallback(cbLoopDone)

    reactor.run()


if __name__ == "__main__":
    main()
