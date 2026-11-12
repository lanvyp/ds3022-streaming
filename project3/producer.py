"""producer.py: Bluesky posts (Jetstream WebSocket) -> Kafka topic bluesky_posts.

  python producer.py              # live, from now
  python producer.py <time_us>    # replay from a past time (Jetstream cursor, card D5)

Every message is wrapped in the same envelope: {"action": "live", "data": <Jetstream event>}
Key: did (the account), so all events of one account stay in order on one partition.
"""
import asyncio
import json
import logging
import sys

from quixstreams import Application
from tornado.httpclient import HTTPRequest
from tornado.websocket import websocket_connect

BROKER = "localhost:19092"
TOPIC = "bluesky_posts"
WSS_URL = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"
USER_AGENT = "DS3022-Project3/1.0 (UVA in Valencia student project)"
LOG_EVERY = 1000

sent = 0
last_time_us = None   # time of the last event we sent: used to resume after a drop


def send(producer, message):
    """One Kafka record per message, keyed by did."""
    global sent, last_time_us
    data = message["data"]
    key = data.get("did")
    if key is None:
        logging.warning("No did: sent without a key")
    producer.produce(topic=TOPIC, key=key, value=json.dumps(message))
    sent = sent + 1
    if data.get("time_us"):
        last_time_us = data.get("time_us")
    if sent % LOG_EVERY == 0:
        logging.info("Sent %d messages", sent)


async def live(producer, start_cursor=None):
    """PUSH over a WebSocket, with a reconnect loop."""
    global last_time_us
    last_time_us = start_cursor
    while True:
        url = WSS_URL
        if last_time_us is not None:
            # resume where we stopped (Jetstream keeps a replay window)
            url = url + "&cursor=" + str(last_time_us)
        try:
            logging.info("Connecting to %s", url)
            request = HTTPRequest(url, headers={"User-Agent": USER_AGENT})
            ws = await websocket_connect(request, ping_interval=15)
            while True:
                text = await ws.read_message()
                if text is None:
                    logging.warning("The server closed the connection")
                    break
                try:
                    send(producer, {"action": "live", "data": json.loads(text)})
                except (ValueError, KeyError, TypeError, AttributeError) as e:
                    logging.warning("Skipped a message: %r", e)
        except Exception as e:
            logging.warning("Connection failed: %r", e)
        producer.flush()
        logging.info("Reconnecting in 5 s")
        await asyncio.sleep(5)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start_cursor = None
    if len(sys.argv) > 1:
        start_cursor = int(sys.argv[1])
    app = Application(broker_address=BROKER, loglevel="WARNING")
    with app.get_producer() as producer:
        try:
            asyncio.run(live(producer, start_cursor))
        except KeyboardInterrupt:
            logging.info("Stopped after %d messages", sent)


if __name__ == "__main__":
    main()