import requests
import time
import json
import logging
from quixstreams import Application

def get_weather():
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 38.0135025,
            "longitude": -78.728928,
            "current": "temperature_2m",
        },
    )

    return response.json()


def main():
    app = Application(
        broker_address="localhost:19092",
        loglevel="DEBUG",
    )

    with app.get_producer() as producer:
        while True:
            weather = get_weather()
            logging.debug("Got weather: %s", weather)
            producer.produce(
                topic="weather_data_demo",
                key="CharlottesvilleWeather",
                value=json.dumps(weather),
            )
            logging.info("Produced. Sleeping...")
            time.sleep(10)


if __name__ == "__main__":
  try:
    logging.basicConfig(level="DEBUG")
    main()
  except KeyboardInterrupt:
    pass
