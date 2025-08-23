# 🚖 Weather Impact on Taxi Demand – Airflow Data Pipeline  

## 📌 Project Overview  
This project demonstrates how to build an **automated, production-style data pipeline** using **Apache Airflow**.  
The pipeline ingests **NYC Taxi trip data (CSV)** and **daily weather data (OpenWeather API)**, joins them, and stores the results in **PostgreSQL** for downstream analysis.  

Portfolio Highlight:  
> “Automated daily pipeline combining API + historical data using Airflow for orchestration.”  

---

## ⚙️ Tech Stack  
- **Python** 🐍 – data processing & API calls  
- **Apache Airflow** – DAG scheduling & orchestration  
- **PostgreSQL** – data warehouse / storage  
- **Docker & docker-compose** – containerized environment  
- **OpenWeather API** – external weather data  

---

## 📂 Project Structure  
```
.
├── docker-compose.yml
├── requirements.txt
├── README.md
├── .env                   # you create this with secrets/config
├── dags/
│   └── taxi_weather_dag.py
├── data/
│   └── nyc-taxi-trip-duration.csv  # sample file (see notes on daily filenames)
└── scripts/
    ├── __init__.py
    ├── extract_taxi_data.py
    ├── fetch_weather_data.py
    ├── join_and_transform.py
    └── load_to_postgres.py
```

---

## Getting Started

1) Prerequisites
- Docker Desktop and docker-compose
- OpenWeather API key: https://openweathermap.org/api

2) Create .env
```
WEATHER_API_KEY=YOUR_OPENWEATHER_KEY
WEATHER_CITY=New York,US
```

3) Wire env vars and data volume to Airflow containers
- In docker-compose.yml, for both webserver and scheduler services:
  - Add environment entries so your scripts can read the API settings:
    - WEATHER_API_KEY=${WEATHER_API_KEY}
    - WEATHER_CITY=${WEATHER_CITY}
  - Mount the data folder so the DAG can read taxi CSVs:
    - volumes: - ./data:/data

4) Start the stack
```
docker compose up -d
```
- UI: http://localhost:8080 (user: admin, pass: admin)

5) Run the pipeline
- In the Airflow UI, unpause the DAG: taxi_weather_pipeline
- Trigger a run or wait for the daily schedule

---

## How It Works
- Task 1: extract_taxi_data → reads daily taxi CSV and writes /tmp/taxi_data.csv
- Task 2: fetch_weather_data → fetches weather via OpenWeather and writes /tmp/weather_data.csv
- Task 3: join_and_transform → merges by date and writes /tmp/final_data.csv
- Task 4: load_to_postgres → appends final data to Postgres table taxi_weather

Pipeline graph:

![Airflow DAG – taxi_weather_pipeline](assets/taxi-weather-pipeline.png)

Notes
- Weather API: the script fetches current weather and stamps it to “yesterday” to align with taxi data.
- File paths are inside the container; /tmp is ephemeral per container run.

---

## Configuration
- WEATHER_API_KEY and WEATHER_CITY are read by scripts/fetch_weather_data.py from environment variables.
- Postgres connection (inside containers): postgresql://airflow:airflow@postgres:5432/airflow
- Table name: taxi_weather (if_exists="append")

---

## Input Data Expectations
- extract_taxi_data expects a dated CSV under /data with pattern:
  - /data/nyc-taxi-trip-durationYYYY-MM-DD.csv (example: nyc-taxi-trip-duration2025-08-22.csv)
- For quick testing you can copy the sample to a dated filename:
  - cp data/nyc-taxi-trip-duration.csv data/nyc-taxi-trip-duration$(date -v-1d +%Y-%m-%d).csv  # macOS

Ensure the data directory is mounted into the containers at /data as noted above.

---

## Troubleshooting
- WEATHER_API_KEY is not set → fetch_weather_data will raise a RuntimeError. Ensure .env is created and env vars are passed into containers via docker-compose.yml.
- File not found: /data/nyc-taxi-trip-durationYYYY-MM-DD.csv → verify the dated file exists and that ./data is mounted to /data in both webserver and scheduler.
- Can’t access UI on :8080 → ensure the container is healthy and the port isn’t in use.
- Postgres table not appearing → confirm task_load_to_postgres ran and that the connection string is reachable from within containers.
