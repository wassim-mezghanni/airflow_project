import pandas as pd
from datetime import datetime, timedelta

def extract_taxi_data():
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    df = pd.read_csv(f"/data/nyc-taxi-trip-duration{yesterday}.csv")
    df.to_csv("/tmp/taxi_data.csv", index=False)
    print(f"Taxi data extracted for {yesterday}")
