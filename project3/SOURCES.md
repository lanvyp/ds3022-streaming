# Sources menu

Taste a source before you choose it: `python probe.py <name>` runs for 60 seconds **without Kafka** and prints three real messages, the fields (and which are sometimes missing), the measured rate, the time to 50,000 records, and hints for your Source Design Card.

| Source | `probe.py` name | Method (as in class) | Expected rate | 50K by | Used last year | Status |
|---|---|---|---|---|---|---|
| Crypto trades (Coinbase) | `coinbase` | WebSocket push (Mon 02 Nov) | 5.7/s with 3 products (05 Nov, afternoon); quieter at night | about 1-2 h live with 6 products | 1 project (1.8M records) | ☐ tested in class |
| Bluesky posts (Jetstream) | `bluesky` | WebSocket push (Mon 02 Nov) | high, even for posts only | minutes | 6 projects | ☐ tested in class |
| Aircraft positions (OpenSky) | `opensky` | REST polling (Thu 22 Oct weather) | thousands of aircraft per request | about 10 requests (10 min at home) | 3 projects | ☐ tested in class |

All three use only the two methods from class: **polling or paging a REST API** with `requests` (the weather producer on Thu 22 Oct, `history()` on Mon 02 Nov) and **a WebSocket** with tornado (`live()` on Mon 02 Nov).

The **Status** column is ticked by the instructor once the probe has run on the classroom Wi-Fi.

## The three cards

### Crypto trades (Coinbase Exchange) · WebSocket
- **What arrives:** one `match` message per trade for the products you subscribe to.
- **Key:** `product_id`. **Identity:** `product_id` + `trade_id`; `sequence` gaps reveal dropped messages.
- **Catches:** subscribe within 5 seconds of connecting; the first messages are `subscriptions` and `last_match` (not new trades); Coinbase warns that messages can be dropped from this channel; quieter at night.
- **Backfill:** none on the WebSocket. `probe.py` subscribes to six products; add more to go faster. Start collecting on the evening of Session 1: the messages wait in the topic until your lander reads them in Session 2.
- **Questions:** trades per minute vs. price moves; buy vs. sell pressure; trade sizes by product.

### Bluesky posts (Jetstream) · WebSocket
- **What arrives:** new and deleted posts from the whole network, as JSON (`did`, `time_us`, `kind`, `commit.operation`, `commit.collection`, `commit.rkey`, `commit.record`).
- **Key:** `did` (one account's posts in order). **Identity:** `did` + `commit.rkey` (+ operation).
- **Catches:** fast: filter early (the probe already asks for posts only); deleted posts have no `record`; text and languages are in `commit.record`.
- **Backfill:** a limited replay with `?cursor=<time_us>`.
- **Questions:** posts by language and hour; replies vs. originals; links and hashtags.

### Aircraft positions (OpenSky Network) · REST polling
- **What arrives:** one GET to `https://opensky-network.org/api/states/all` returns every aircraft in the air right now, as lists of values (`icao24`, `callsign`, `origin_country`, `time_position`, `last_contact`, `longitude`, `latitude`, `baro_altitude`, `on_ground`, `velocity`, ...). Your producer turns each list into a dict (`STATE_FIELDS` in `probe.py`) and sends one Kafka record per aircraft.
- **Key:** `icao24` (all positions of one aircraft in order). **Identity:** `icao24` + `time_position`.
- **Duplicates on purpose:** polling faster than every 10 seconds returns the same snapshot again, and an aircraft that has not reported since the last poll comes back unchanged. It is the same lesson as the weather producer, where the reading only changed every 15 minutes.
- **Catches:** without an account you get 400 credits a day **per IP address**, and a world-wide request costs 4 (100 requests a day). Everyone in the classroom shares one IP: probe once in class, collect at home. HTTP 429 means the credits are used up (the header `X-Rate-Limit-Retry-After-Seconds` says how long to wait). `callsign` has trailing spaces; positions and `time_position` can be null.
- **Backfill:** none without an account, but 10 requests (one a minute) usually pass 50,000 records.
- **Questions:** aircraft in the air by hour; countries of origin over Europe; climbing vs. descending by area; on the ground vs. flying.
- **Start from:** the Thu 22 Oct weather producer (a loop with `requests.get` and `time.sleep`) + `probe_poll()` in `probe.py`.

## Your own idea

Bring it to Session 1 (Thu 05 Nov) with a working probe:

```
python probe.py ws   <url> '<subscribe JSON>'
python probe.py rest <url>
```

Keep to the two methods from class: a WebSocket, or a REST API that you poll or page through.

Use it only if, by minute 50 of Session 1 (Thu 05 Nov), you can tick the self-check in `SOURCE_CARD.md`: messages arrive in class, you can name a key and a duplicate identity, and the volume maths reaches 50,000 by the start of Session 3 (Thu 12 Nov). No API keys that take days to issue. A REST API is fine if your producer pages through it into Kafka and handles rate limits; Kafka is still required.

Other ideas from last year (all in `project-3-examples/`) need a free key or account, so get it before Session 1: Washington DC bus positions (WMATA key), Alpaca stock trades (free account), GitHub REST API (token), NYT Archive API (free key), YouTube comments (API key).
