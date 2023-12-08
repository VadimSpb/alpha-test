import os
import json
import requests
from airflow.models import Variable


def _get_city_coordinates(city_name, appid):
    url = 'http://api.openweathermap.org/data/2.5/weather'
    params = {'q': city_name, 'appid': appid}
    response = requests.get(url, params=params)
    data = response.json()
    lat = data['coord']['lat']
    lon = data['coord']['lon']
    return lat, lon


def get_weather_forecast(lat, lon, appid):
    params = {
        'lat': lat,
        'lon': lon,
        'exclude': 'minutely,alerts,current',
        'units': 'metric',
        'appid': appid
    }
    url = f"https://api.openweathermap.org/data/2.5/onecall"
    response = requests.get(url, params=params)
    return response.json()


def openweathermap_provider(city_name, appid):
    lat, lon = _get_city_coordinates(city_name, appid)
    response = get_weather_forecast(lat, lon, appid)
    forecast_data = response['hourly']
    file_path = Variable.get("bronze_tier_path")
    full_path = os.path.join(file_path, f"{city_name}.json")
    with open(full_path, 'a') as json_file:
        json.dump(forecast_data, json_file, indent=4)
        json_file.write('\n')
