from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from kafka import KafkaProducer

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_weather_data():
    cities = ['London', 'New York', 'Tokyo', 'Paris', 'Sydney']
    api_key = os.getenv('OPENWEATHER_API_KEY')
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    for city in cities:
        url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
        response = requests.get(url)
        data = response.json()
        
        weather_data = {
            'city_name': city,
            'temperature': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'pressure': data['main']['pressure'],
            'timestamp': datetime.utcnow().isoformat()
        }
        
        producer.send('weather_data', weather_data)
    
    producer.close()

dag = DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Fetch weather data and send to Kafka',
    schedule_interval=timedelta(hours=1),
    catchup=False
)

fetch_weather_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    dag=dag
)