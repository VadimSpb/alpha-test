import os
import json
import csv
from datetime import datetime
from airflow.models import Variable

def convert_pressure(hPa):
    return round(hPa * 0.750062, 0)

def is_rainy(weather_data):
    for weather in weather_data:
        if 'rain' in weather['main'].lower():
            return 1
    return 0

def process_json_to_csv(city_name):
    json_file = os.path.join(Variable.get("bronze_tier_path"), f"{city_name}.json")
    with open(json_file, 'r') as file:
        data = json.load(file)

    csv_file = os.path.join(Variable.get("silver_tier_path"), f"{city_name}.csv")
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['city', 'date', 'hour', 'temperature_c', 'pressure_mm', 'is_rainy'])

        for item in data:
            dt = datetime.utcfromtimestamp(item['dt'])
            date = dt.strftime('%d.%m.%Y')
            hour = dt.strftime('%H')
            temperature_c = item['temp']
            pressure_mm = int(convert_pressure(item['pressure']))
            rainy = is_rainy(item['weather'])
            writer.writerow([city_name, date, hour, temperature_c, pressure_mm, rainy])

