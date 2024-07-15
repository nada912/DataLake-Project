from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from supabase import create_client, Client
import os
import requests

def get_spotify_token():
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': os.getenv('SPOTIFY_CLIENT_ID'),
        'client_secret': os.getenv('SPOTIFY_CLIENT_SECRET'),
    })
    return auth_response.json().get('access_token')

def fetch_spotify_data(endpoint, token):
    headers = {
        'Authorization': f'Bearer {token}'
    }
    response = requests.get(endpoint, headers=headers)
    return response.json()

def fetch_and_store_data():
    supabase_url = os.getenv('DATABASE_URL')
    supabase_key = os.getenv('JWT_SECRET_KEY')
    supabase: Client = create_client(supabase_url, supabase_key)
    
    token = get_spotify_token()
    # Example endpoint to fetch artists
    endpoint = 'https://api.spotify.com/v1/artists/{id}'
    data = fetch_spotify_data(endpoint, token)
    # Store data in the Supabase database
    supabase.table('MAIN_ARTIST').insert(data).execute()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 15),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spotify_data_dag',
    default_args=default_args,
    description='A simple DAG to fetch data from Spotify API',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='fetch_and_store_data',
    python_callable=fetch_and_store_data,
    dag=dag,
)

run_etl
