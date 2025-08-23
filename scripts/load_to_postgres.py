import pandas as pd
from sqlalchemy import create_engine

def load_to_postgres():
    df = pd.read_csv("/tmp/final_data.csv")
    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
    df.to_sql("taxi_weather", engine, if_exists="append", index=False)
    print("Data loaded to Postgres")
