import pandas as pd

def join_and_transform():
    taxi_df = pd.read_csv("/tmp/taxi_data.csv")
    weather_df = pd.read_csv("/tmp/weather_data.csv")

    taxi_df["date"] = pd.to_datetime(taxi_df["pickup_datetime"]).dt.date
    weather_df["date"] = pd.to_datetime(weather_df["date"]).dt.date

    merged = taxi_df.merge(weather_df, on="date", how="left")
    merged.to_csv("/tmp/final_data.csv", index=False)
    print("Datasets joined and transformed")
