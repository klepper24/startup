import json
from datetime import timedelta
from datetime import datetime
from dateutil import tz
import requests
import logging
import os

from airflow.decorators import dag, task
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import pandas as pd

 
default_args = {
    'owner': 'Michal Klepacki',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 1),
    'email': ['m.j.klepacki@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

cities = { 'Białystok': [23.15, 53.1333],
           'Bydgoszcz': [18.0076, 53.1235],
           'Gdańsk': [18.6464, 54.3521],
           'Gorzów Wielkopolski': [15.2288, 52.7368],
           'Katowice': [19.0275, 50.2584],
           'Kielce': [20.6275, 50.8703],
           'Kraków': [19.9167, 50.0833],
           'Lublin': [23, 51],
           'Łódź': [19.5, 51.5],
           'Olsztyn': [20.4942, 53.7799],
           'Opole': [18, 50.5],
           'Poznań': [16.9299, 52.4069],
           'Rzeszów': [21.999, 50.0413],
           'Szczecin': [14.553, 53.4289],
#           'Toruń': [18.5981, 53.0137],
           'Warszawa': [21.0118, 52.2298],
           'Wrocław': [17.03, 51.1],
           'Zielona Góra': [15.5064, 51.9355]
           }
appid = api_key = Variable.get("api_key")
units = 'metric'
 
@dag(
	dag_id='weather_and_forecast_collector', 
	description='Collects data about current weather and forecast',
    schedule_interval='@hourly', 
	start_date=datetime(2022, 6, 1),
    default_args=default_args,
    catchup=False
) 
def saveWeatherAndForecast():

    task_is_weather_api_active = HttpSensor(
        task_id='is_weather_api_active',
        http_conn_id='open_weather',
        endpoint='data/2.5/weather',
        request_params={'q': 'Wrocław', 'appid' : appid}
    )
    
    task_is_forecast_api_active = HttpSensor(
        task_id='is_forecast_api_active',
        http_conn_id='open_weather',
        endpoint='data/2.5/forecast',
        request_params={'q': 'Wrocław', 'appid' : appid}
    )

    @task(task_id='save_weather',)
    def saveWeather():
        to_zone = tz.gettz('Europe/Warsaw')
        today_time = datetime.now().astimezone(to_zone)
        
        date = today_time.strftime("%Y%m%d")
         
        for key in cities:
            url = 'http://api.openweathermap.org/data/2.5/weather?appid='+ appid + '&q=' + key + '&units=' + units
            for i in range(5):
                r = requests.get(url)
                status = r.status_code
                logging.info(f'Status: {status}')
                if status == 200:

                    json_response = r.json()
                    df = pd.json_normalize(
                        json_response,
                        record_path=['weather'],
                        meta=['name', ['main', 'temp'], ['main', 'feels_like'], ['main', 'temp_min'], ['main', 'temp_max'], ['main', 'pressure'], ['main', 'humidity'],
                               'visibility', 'wind', 'clouds', 'rain', 'snow', 'dt', ['sys', 'sunrise'], ['sys', 'sunset']],
                        errors='ignore'
                    )
                    df.drop(columns=['id', 'icon'], inplace=True)
                    df = df.rename(columns={'main': 'weather_category', 'description': 'weather_description',
                                            'main.temp': 'temp', 'main.feels_like': 'feels_like',
                                            'main.temp_min': 'temp_min', 'main.temp_max': 'temp_max',
                                            'main.pressure': 'pressure', 'main.humidity': 'humidity', 'dt': 'timestamp',
                                            'sys.sunrise': 'sunrise', 'sys.sunset': 'sunset',
                                            'name': 'city'})
                    df = df.apply(lambda x: datetime.fromtimestamp(int(x)).astimezone(to_zone) if x.name in ['timestamp', 'sunrise', 'sunset'] else x)
                    df['city'] = df['city'].str.replace('Voivodeship', '')
                    column_names = [
                        'weather_category',
                        'weather_description',
                        'city',
                        'temp',
                        'feels_like',
                        'temp_min',
                        'temp_max',
                        'pressure',
                        'humidity',
                        'clouds',
                        'wind',
                        'visibility',
                        'rain',
                        'snow',
                        'timestamp',
                        'sunrise',
                        'sunset'
                    ]
                    df = df.reindex(columns=column_names)
                    
                    # saving json to file
                    filename = f'dags/weather/weather_{date}.csv'
                    os.makedirs(os.path.dirname(filename), exist_ok=True)
                    
                    if not os.path.isfile(filename):
                        df.to_csv(filename, header='column_names', index=None)
                    else:
                        df.to_csv(filename, mode='a', header=False, index=None)
                    break
            
            
    @task(task_id='save_forecast',)
    def saveForecast():
        to_zone = tz.gettz('Europe/Warsaw')
        today_time = datetime.now().astimezone(to_zone)
        
        date = today_time.strftime("%Y%m%d")
         
        for key in cities:
            url = 'http://api.openweathermap.org/data/2.5/forecast?appid='+ appid + '&q=' + key + '&units=' + units
            for i in range(5):
                r = requests.get(url)
                status = r.status_code
                logging.info(f'Status: {status}')
                if status == 200:

                    json_response = r.json()
                    df = pd.json_normalize(json_response)
                    df = df.explode('list')
                    exploded_list = df['list'].apply(pd.Series)
                    df = pd.concat([exploded_list, df], axis=1)
                    df.drop(columns=['list', 'cod', 'message', 'cnt', 'dt'], inplace=True)
                    df = df.explode('weather')
                    exploded_weather = df['weather'].apply(pd.Series)
                    exploded_weather = exploded_weather.rename(columns={"main": "weather_category"})
                    df = pd.concat([exploded_weather, df], axis=1)
                    df.drop(columns=['weather', 'id', 'icon'], inplace=True)
                    exploded_main = df['main'].apply(pd.Series)
                    df = pd.concat([exploded_main, df], axis=1)
                    df.drop(columns=['main'], inplace=True)
                    if 'rain' not in df: df.insert(df.columns.get_loc('sys'), 'rain', '')
                    if 'snow' not in df: df.insert(df.columns.get_loc('sys'), 'snow', '')
                    df.drop(columns=['city.id', 'city.country', 'city.population'], inplace=True)
                    df = df.rename(columns={'description': 'weather_description',
                                            'city.coord.lon': 'lon',
                                            'city.coord.lat': 'lat',
                                            'city.name': 'city',
                                            'city.timezone': 'timezone',
                                            'city.sunrise': 'sunrise',
                                            'city.sunset': 'sunset',
                                            'pop': 'probability_of_precipitation',
                                            'dt_txt': 'timestamp_of_data_forecasted'})
                    df['city'] = df['city'].str.replace('Voivodeship', '')
                    df['forecast_timestamp'] = today_time
                    column_names = [
                        'temp',
                        'feels_like',
                        'temp_min',
                        'temp_max',
                        'pressure',
                        'sea_level',
                        'grnd_level',
                        'humidity',
                        'temp_kf',
                        'weather_category',
                        'weather_description',
                        'clouds',
                        'wind',
                        'visibility',
                        'probability_of_precipitation',
                        'rain',
                        'snow',
                        'sys',
                        'timestamp_of_data_forecasted',
                        'city',
                        'lat',
                        'lon',
                        'timezone',
                        'sunrise',
                        'sunset',
                        'forecast_timestamp'
                    ]
                    df = df.reindex(columns=column_names)
                    
                    # saving json to file
                    filename = f'dags/forecast/forecast_{date}.csv'
                    os.makedirs(os.path.dirname(filename), exist_ok=True)
                    
                    if not os.path.isfile(filename):
                        df.to_csv(filename, header='column_names', index=None)
                    else:
                        df.to_csv(filename, mode='a', header=False, index=None)
                    break            
 

    task_is_weather_api_active >> saveWeather() >> task_is_forecast_api_active >> saveForecast()

dag = saveWeatherAndForecast()
