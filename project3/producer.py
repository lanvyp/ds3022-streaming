"""Earthquakes -> Kafka topic "quakes".

  python quakes_producer.py history 7   # PULL: the last 7 days from the EMSC catalogue
  python quakes_producer.py live        # PUSH: new and updated events from the EMSC WebSocket

Adapted from Professor Magee's stream-seismic.py (learn-kafka).
"""
import asyncio
import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from quixstreams import Application
from tornado.websocket import websocket_connect

BROKER = "127.0.0.1:19092"
TOPIC = "quakes"
FDSN_URL = "https://www.seismicportal.eu/fdsnws/event/1/query"
WSS_URL = "wss://www.seismicportal.eu/standing_order/websocket"


def send(producer, message):
    """One Kafka record per message. Key = the event id, so every
    version of the same earthquake goes to the same partition, in order."""
    unid = message["data"]["properties"]["unid"]
    producer.produce(topic=TOPIC, key=unid, value=json.dumps(message))
    return unid


def history(producer, days):
    """PULL: ask the catalogue for one day at a time (far below the 20,000 limit)."""
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    total = 0
    for back in range(days - 1, -1, -1):  # the last N days, today included
        start = today - timedelta(days=back)
        end = start + timedelta(days=1)
        r = requests.get(
            FDSN_URL,
            params={
                "start": start.strftime("%Y-%m-%dT%H:%M:%S"),
                "end": end.strftime("%Y-%m-%dT%H:%M:%S"),
                "format": "json",
                "limit": 20000,
                "orderby": "time-asc",
            },
            timeout=60,
        )
        if r.status_code == 204:  # no events that day
            continue
        r.raise_for_status()
        features = r.json()["features"]
        for feature in features:
            send(producer, {"action": "history", "data": feature})
        total += len(features)
        logging.info("%s  %4d events   total %d", start.date(), len(features), total)
        time.sleep(1)  # be polite to a free public service
    logging.info("History done: %d events sent to %s", total, TOPIC)


async def live(producer):
    """PUSH: keep one connection open; EMSC sends each new or updated event."""
    ws = await websocket_connect(WSS_URL, ping_interval=15)
    logging.info("Connected. Waiting for earthquakes (often about one a minute)...")
    while True:
        text = await ws.read_message()
        if text is None:
            logging.warning("The server closed the connection")
            break
        try:
            message = json.loads(text)
            unid = send(producer, message)
            p = message["data"]["properties"]
            logging.info("%-7s %s  M%s  %s", message.get("action"), unid, p.get("mag"), p.get("flynn_region"))
        except (ValueError, KeyError, TypeError) as e:
            logging.warning("Skipped a message I could not read: %r", e)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "live"
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        if mode == "history":
            history(producer, int(sys.argv[2]) if len(sys.argv) > 2 else 7)
        else:
            asyncio.run(live(producer))


if __name__ == "__main__":
    logging.basicConfig(level="INFO", format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
    try:
        main()
    except KeyboardInterrupt:
        pass
