import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# Read API key and city from environment to avoid hardcoding secrets
API_KEY = os.getenv("WEATHER_API_KEY")
CITY = os.getenv("WEATHER_CITY", "New York,US")


def fetch_weather_data():
    if not API_KEY:
        raise RuntimeError("WEATHER_API_KEY is not set. Provide it via environment variables.")

    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    url = (
        "http://api.openweathermap.org/data/2.5/weather"
        f"?q={CITY}&appid={API_KEY}&units=metric"
    )

    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame([
        {
            "date": yesterday,
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather": data["weather"][0]["main"],
        }
    ])
    df.to_csv("/tmp/weather_data.csv", index=False)
    print(f"Weather data fetched for {CITY}")
