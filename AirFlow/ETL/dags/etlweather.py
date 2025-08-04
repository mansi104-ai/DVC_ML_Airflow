from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

## Latitude and Longitude for the desired location (London in this case)
LATITUDE = '51.074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
  'owner': 'airflow',
  'start_date': days_ago(1)

}
##DAG
with DAG(dag_id = 'weather_etl_pipeline',
         default_args = default_args,
         schedule_interval = '@daily',
         catchup = False) as dags:
  
  @task()
  def extract_weather_data():
    """Extract the weather data from Open-Meteor api"""
    # Use HTTP hook to get the weather data to get connection details from Airflow connection

    http_hook = HttpHook(http_conn_id = API_CONN_ID, method= 'GET')

    #Build the API endpoint
    ## https://api.open-meteo.com/v1/forecast?latitude=51.074&longitude=-0.1278&current_weather=true
    endpoint = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
    
    ## Make the request via the HTTP Hook
    response = http_hook.run(endpoint)

    if response.status_code == 200:
      return response.json()
    else:
      raise Exception(f"Failed to fetch the weather data: {response.status_code}")

  
  @task()
  def transform_weather_data(weather_data):
    """Transform the weather data """
    current_weather = weather_data['current_weather']
    transformed_data = {
      'latitude' : LATITUDE,
      'longitude' : LONGITUDE,
      'temperature' : current_weather['temperature'],
      'windspeed': current_weather['wind_speed'],
      'winddirection': current_weather['winddirection'],
      'weathercode' : current_weather['weathercode']
    }
    return transform_weather_data
  
  @task
  def load_weather_data(transformed_data):
    """Load the transformed data into PostGresSQL"""
    pg_hook = PostgresHook(postgress_conn_id = POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    #Create table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS weather_data(
                   latitude FLOAT,
                   longitude FLOAT,
                   temperature FLOAT,
                   windspeed FLOAT,
                   winddirection FLOAT,
                   weathercode INT)
            
                  """)
    
    ## Insert transformed data into the table
    cursor.execute("""
    INSERT INTO weather data(latitude, longitude, temperature, windspeed, winddirection, weathercode)
    VALUES(%s,%s,%s,%s,%s,%s)
                   """,(
                     transformed_data['latitude'],
                     transformed_data['longitude'],
                     transformed_data['temperature'],
                     transformed_data['windspeed'],
                     transformed_data['winddirection'],
                     transformed_data['weathercode'],
                   ))
    conn.commit()
    cursor.close()


    ## DAG workflow - ETL pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
