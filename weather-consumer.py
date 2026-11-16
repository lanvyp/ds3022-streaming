from quixstreams import Application
import json
import time

def main():
    app = Application(
        # set broker(s)
        broker_address="localhost:19092",

        # set the logging level
        loglevel="DEBUG",

        # define the consumer group
        consumer_group="weather_reader",

        # processing guarantees: 
        #   - exactly-once   - msg will be processed exactly once
        #   - at-least-once  - may be processed more than once
        #   KAFKA native but not in QuixStreams:
        #   - at-most-once   - if process fails it will not be retried
        processing_guarantee="exactly-once",

        # set the offset reset - either earliest || latest
        auto_offset_reset="earliest",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["weather_data_demo"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting...")
            elif msg.error() is not None:
                raise Exception(msg.error())
            else:
                key = msg.key().decode("utf8")
                value = json.loads(msg.value())
                offset = msg.offset()

                print(f"{offset} {key} {value}")
                consumer.store_offsets(msg)
                time.sleep(10)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
