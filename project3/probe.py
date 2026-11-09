"""probe.py: taste a data source for 60 seconds, without Kafka.

  python probe.py                  # list the sources on the menu
  python probe.py coinbase         # WebSocket (push), trades ("matches")
  python probe.py bluesky          # WebSocket (push), Jetstream, posts only
  python probe.py opensky          # REST polling (pull), aircraft positions: 2 requests

  Your own idea (WebSocket or REST only, the methods from class):
  python probe.py ws   <url> ['<subscribe JSON>']
  python probe.py rest <url>       # one GET request: shows the structure

  Add a number of seconds at the end to probe for longer, e.g.  python probe.py coinbase 120

What it prints: three real messages, the fields that appear (and how often),
the measured rate, how long 50,000 records would take, and hints for your
Source Design Card (key, duplicate identity, catches).

You may reuse the connection code below in your producer.py: say so in your README.
Needs only what the 22 Oct class installed (requests, tornado).
"""
import asyncio
import json
import sys
import time
from collections import Counter

import requests

TARGET = 50_000
USER_AGENT = "DS3022-Project3-probe/1.0 (UVA in Valencia student project)"

PRESETS = {
    "coinbase": {
        "kind": "ws",
        "url": "wss://ws-feed.exchange.coinbase.com",
        "subscribe": {"type": "subscribe", "product_ids": ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD", "ADA-USD"],
                      "channels": ["matches"]},
        "protocol": "WebSocket (push). Public market data; you must subscribe within 5 seconds of connecting.",
        "key": "product_id (every trade of one product in order)",
        "identity": "product_id + trade_id. sequence numbers are per product: a gap means a dropped message.",
        "catches": "The first message is 'subscriptions', then one 'last_match' per product: not new trades. "
                   "Coinbase warns messages can be dropped from this channel. Rate depends on the market (quiet at night).",
        "history": "No replay on the WebSocket. Collect live, starting on the evening of Session 1; add products to go faster.",
        "questions": "Trades per minute vs. price moves? Buy vs. sell pressure? Trade size distribution by product?",
    },
    "bluesky": {
        "kind": "ws",
        "url": "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
        "protocol": "WebSocket (push). Jetstream turns the Bluesky firehose into plain JSON.",
        "key": "did (all events of one account in order) or did + commit.rkey",
        "identity": "did + commit.rkey (+ commit.operation: a post can be created then deleted).",
        "catches": "Very fast even for posts only: filter early or the topic grows quickly. Text is in commit.record.text, "
                   "language in commit.record.langs. Deleted posts arrive without a record.",
        "history": "Jetstream can replay a limited window with ?cursor=<time_us> (check how far back on the day).",
        "questions": "Posting rate by language and hour? Share of replies vs. original posts? Most-mentioned links or hashtags?",
    },
    "opensky": {
        "kind": "poll",
        "url": "https://opensky-network.org/api/states/all",
        "protocol": "REST polling (pull), like the Thu 22 Oct weather producer: one GET returns every aircraft in the air right now.",
        "key": "icao24 (the aircraft's transponder id: all positions of one aircraft in order)",
        "identity": "icao24 + time_position. Polling faster than every 10 s returns the same snapshot again, "
                    "and a plane that has not reported since the last poll comes back unchanged: both are the same thing.",
        "catches": "Each state is a list, not a dict: turn it into a dict with STATE_FIELDS. No account: 400 credits per day "
                   "per IP address, and a world-wide request costs 4 (100 requests a day). The class shares one IP: "
                   "probe once in class, collect at home. HTTP 429 = credits used up (the header says how long to wait). "
                   "callsign has trailing spaces; time_position and positions can be null.",
        "history": "No: without an account you only get the current snapshot. Each request returns thousands of aircraft, "
                   "so 50,000 needs only about 10 requests (one a minute for 10 minutes).",
        "questions": "Aircraft in the air by hour of day? Countries of origin over Europe? Climbing vs. descending by area? Planes on the ground vs. flying?",
    },
}


# ---------- what we learn from the messages ----------

class Tally:
    def __init__(self, seconds):
        self.seconds = seconds
        self.start = time.monotonic()
        self.count = 0
        self.bad = 0
        self.samples = []
        self.fields = Counter()
        self.progress = True  # print the live rate every 500 messages

    def done(self):
        return time.monotonic() - self.start >= self.seconds

    def add(self, text):
        try:
            obj = json.loads(text)
        except (ValueError, TypeError):
            self.bad += 1
            return
        self.count += 1
        if len(self.samples) < 3:
            self.samples.append(obj)
            print(f"  message {len(self.samples)} received")
        if self.count <= 500:
            for path in paths(obj):
                self.fields[path] += 1
        if self.progress and self.count % 500 == 0:
            print(f"  {self.count} messages, {self.rate():.1f}/s")

    def rate(self):
        return self.count / max(time.monotonic() - self.start, 1e-9)


def paths(obj, prefix=""):
    """All field paths in a JSON object, e.g. meta.id, commit.record.text."""
    out = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            out.append(p)
            if isinstance(v, dict):
                out.extend(paths(v, p))
    return out


def shorten(obj, limit=900):
    text = json.dumps(obj, ensure_ascii=False, indent=2)
    return text if len(text) <= limit else text[:limit] + "\n  ... (cut)"


def report(name, tally, per="second", rate=None, note="", eta=True):
    preset = PRESETS.get(name, {})
    print("\n" + "=" * 70)
    print(f"PROBE REPORT: {name}")
    print("=" * 70)
    for i, s in enumerate(tally.samples, 1):
        print(f"\n--- sample message {i} ---\n{shorten(s)}")
    seen = min(tally.count, 500)
    if seen:
        print(f"\n--- fields in the first {seen} messages (how often each appears) ---")
        for f, n in sorted(tally.fields.items(), key=lambda x: (-x[1], x[0]))[:30]:
            flag = "" if n == seen else "   <- missing in some messages"
            print(f"  {100 * n / seen:5.1f}%  {f}{flag}")
    print("\n--- volume ---")
    print(f"  messages: {tally.count}   not JSON: {tally.bad}")
    if rate is None:
        rate = tally.rate()
    if not eta:
        pass
    elif rate > 0:
        print(f"  rate: {rate:,.2f} per {per}")
        need = TARGET / rate
        print(f"  50,000 records need about {need:,.1f} {per}s" + (f" ({human(need, per)})" if per == "second" else ""))
    else:
        print(f"  rate: 0 per {per}")
        print("  rate is 0: nothing arrived. Check the URL, your Wi-Fi, or try again for longer.")
    if note:
        print(f"  note: {note}")
    if preset:
        print("\n--- for your Source Design Card ---")
        for label in ("protocol", "key", "identity", "catches", "history", "questions"):
            print(f"  {label:9s} {preset[label]}")
    print("\nCopy the rate, one sample message and your key choice into SOURCE_CARD.md.")


def human(seconds, per):
    if seconds < 120:
        return f"{seconds:.0f} s"
    if seconds < 7200:
        return f"{seconds / 60:.0f} min"
    if seconds < 172800:
        return f"{seconds / 3600:.1f} h"
    return f"{seconds / 86400:.1f} days"


# ---------- the connectors (reuse these in producer.py) ----------

async def probe_ws(url, tally, subscribe=None):
    """WebSocket: one open connection, the server pushes messages."""
    from tornado.httpclient import HTTPRequest
    from tornado.websocket import websocket_connect

    ws = await websocket_connect(HTTPRequest(url, headers={"User-Agent": USER_AGENT}), ping_interval=15)
    if subscribe is not None:
        await ws.write_message(json.dumps(subscribe))
    print(f"Connected. Reading for {tally.seconds} s...")
    while not tally.done():
        left = tally.seconds - (time.monotonic() - tally.start)
        try:
            text = await asyncio.wait_for(ws.read_message(), timeout=max(left, 0.1))
        except asyncio.TimeoutError:
            break
        if text is None:
            print("The server closed the connection.")
            break
        tally.add(text)
    ws.close()


STATE_FIELDS = ["icao24", "callsign", "origin_country", "time_position", "last_contact",
                "longitude", "latitude", "baro_altitude", "on_ground", "velocity", "true_track",
                "vertical_rate", "sensors", "geo_altitude", "squawk", "spi", "position_source", "category"]


def probe_poll(url, tally, requests_made=2, every=15):
    """REST polling: GET, wait, GET again (as the 22 Oct weather producer does)."""
    per_request = []
    for i in range(requests_made):
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
        if r.status_code == 429:
            wait = r.headers.get("X-Rate-Limit-Retry-After-Seconds", "?")
            print(f"HTTP 429: no credits left for this IP address. Try again in {wait} s (or from home).")
            break
        r.raise_for_status()
        body = r.json()
        states = body.get("states") or []
        for s in states:
            state = dict(zip(STATE_FIELDS, s))
            state["snapshot_time"] = body.get("time")
            tally.add(json.dumps(state))
        per_request.append(len(states))
        left = r.headers.get("X-Rate-Limit-Remaining", "?")
        print(f"  request {i + 1}: {len(states):,} aircraft (snapshot time {body.get('time')}), credits left today: {left}")
        if i < requests_made - 1:
            time.sleep(every)
    return sum(per_request) / max(len(per_request), 1)


def probe_rest(url, tally):
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    print(f"HTTP {r.status_code}, {len(r.content):,} bytes, Content-Type {r.headers.get('Content-Type')}")
    for h in ("X-RateLimit-Limit", "X-RateLimit-Remaining", "Retry-After", "Link"):
        if h in r.headers:
            print(f"  {h}: {r.headers[h]}  <- rate limits / paging: read the docs")
    body = r.json()
    items = body if isinstance(body, list) else next((v for v in body.values() if isinstance(v, list)), [body])
    print(f"  {len(items)} records in this response (records per request x requests allowed = your rate)")
    for item in items:
        tally.add(json.dumps(item))


# ---------- main ----------

def main():
    args = sys.argv[1:]
    seconds = 60
    if args and args[-1].isdigit():
        seconds = int(args.pop())
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        print("Menu:", ", ".join(PRESETS))
        return
    name = args[0].lower()
    tally = Tally(seconds)

    if name in ("ws", "rest"):
        if len(args) < 2:
            sys.exit(f"Give a URL:  python probe.py {name} <url>")
        url = args[1]
        if name == "ws":
            sub = json.loads(args[2]) if len(args) > 2 else None
            asyncio.run(probe_ws(url, tally, sub))
        else:
            probe_rest(url, tally)
            report(url, tally, per="request", rate=tally.count,
                   note="records per request; check how many requests per minute/hour you are allowed")
            return
        report(url, tally)
        return

    if name not in PRESETS:
        sys.exit(f"Unknown source '{name}'. Menu: {', '.join(PRESETS)}  (or ws/rest <url>)")
    p = PRESETS[name]
    print(f"Probing {name} ({p['protocol']})")
    if p["kind"] == "ws":
        asyncio.run(probe_ws(p["url"], tally, p.get("subscribe")))
        report(name, tally)
    elif p["kind"] == "poll":
        tally.seconds, tally.progress = 10 ** 9, False  # a fixed number of requests, not time-limited
        per_request = probe_poll(p["url"], tally)
        report(name, tally, per="request", rate=per_request,
               note="records per request. Without an account: 100 world-wide requests a day per IP address. "
                    "Poll every 60 s at home; 10 minutes is about 10 requests")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")
    except Exception as e:  # network errors from requests or tornado
        sys.exit(f"Could not read the source: {type(e).__name__}: {e}\n"
                 "Check the Wi-Fi, the URL, or try another source.")
