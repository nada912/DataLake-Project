from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
from google.cloud import pubsub_v1
from google.cloud import storage
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException
import os
from airflow.hooks.base_hook import BaseHook
from supabase import create_client, Client

# Définir les configurations de base
project_id = 'charming-sonar-424115-n8'
topic_id = 'myPubsub'
bucket_name = 'my-bucket-datalake'
client_id = "a7520394dac34834a7330006d163693d"
client_secret = "de9a269ca89e40c7b2b542ee51d68b97"

# Assurez-vous que les identifiants sont configurés
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"/home/nada/airflow/application_default_credentials.json"
os.environ['GOOGLE_CLOUD_PROJECT'] = project_id

# Initialiser le client Spotify
client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

# Initialiser Google Cloud Pub/Sub client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Fonction pour publier les messages dans Pub/Sub
def publish_message(message):
    future = publisher.publish(topic_path, json.dumps(message).encode('utf-8'))
    future.result()
    print(f'Published message ID: {future.result()}')

# Fonction pour sauvegarder les données dans GCS
def save_to_gcs(data, filename):
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(data)
    print(f'Saved data to {filename} in bucket {bucket_name}')

# Fonction principale pour fetcher, publier et sauvegarder les playlists
def fetch_publish_save_data(**kwargs):
    limit = 50
    common_queries = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                      'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    offset_increment = 10

    all_artists = []
    all_tracks = []
    artist_ids_seen = set()
    track_ids_seen = set()

    for query in common_queries:
        offset = 0
        while offset < 500:
            try:
                # Search for artists
                artists = sp.search(q=query, type='artist', limit=limit, offset=offset)
                for artist in artists['artists']['items']:
                    if artist['id'] not in artist_ids_seen:
                        all_artists.append(artist)
                        artist_ids_seen.add(artist['id'])

                # Search for tracks
                tracks = sp.search(q=query, type='track', limit=limit, offset=offset)
                for track in tracks['tracks']['items']:
                    if track['id'] not in track_ids_seen:
                        all_tracks.append(track)
                        track_ids_seen.add(track['id'])

                offset += offset_increment

            except SpotifyException as e:
                print(f'Spotify API error: {e}')
                break
            except Exception as e:
                print(f'Error: {e}')
                break

    # Save all artists and tracks data to JSON files
    save_to_gcs(json.dumps(all_artists), 'all_artists_data.json')
    save_to_gcs(json.dumps(all_tracks), 'all_tracks_data.json')

    # Convert data to json
    all_artists_json = json.dumps(all_artists)
    all_tracks_json = json.dumps(all_tracks)

    # Push all playlists data to XCom
    kwargs['ti'].xcom_push(key='spotify_artists', value=all_artists_json)
    print("All artists data pushed to XCom successfully.")
    kwargs['ti'].xcom_push(key='spotify_tracks', value=all_tracks_json)
    print("All tracks data pushed to XCom successfully.")

# Fonction pour insérer les données dans Supabase (Artists, Tracks)
def insert_playlists_to_supabase(**kwargs):
    try:
        # Récupérer les données de XCom
        all_artists_json = kwargs['ti'].xcom_pull(key='spotify_artists')
        all_artists_data = json.loads(all_artists_json)
        all_tracks_json = kwargs['ti'].xcom_pull(key='spotify_tracks')
        all_tracks_data = json.loads(all_tracks_json)

        # Clean data
        artist_clean = []
        for artist in all_artists_data:
            artist_info = {
                'main_artist_id': str(artist['id']),
                'main_artist_name': str(artist['name']),
                'main_artist_popularity': str(artist['popularity']),
                'main_artist_genre': str(artist.get('genres'))
            }
            artist_clean.append(artist_info)

        track_clean = []
        for track in all_tracks_data:
            release_date = track.get('album', {}).get('release_date')
            # Validate release_date format and set default if invalid
            try:
                datetime.strptime(release_date, '%Y-%m-%d')
            except (ValueError, TypeError):
                release_date = '1970-01-01'

            track_info = {
                'track_id': str(track['id']),
                'main_artist_id': str(track['artists'][0]['id']),  # Assuming the first artist is the main artist
                'released_date': str(release_date),
                'track_market': str(track.get('available_markets', [])),
                'album_type': str(track.get('album', {}).get('album_type')),
                'track_duration_ms': str(track.get('duration_ms')),
                'total_tracks': str(track.get('album', {}).get('total_tracks')),
                'track_name': str(track['name']),
                'track_popularity': str(track['popularity']),
                'other_artists': str(','.join([artist['name'] for artist in track['artists'][1:]])) if len(track['artists']) > 1 else None
            }
            track_clean.append(track_info)

        # Récupérer la connexion Supabase depuis Airflow
        conn = BaseHook.get_connection('supabase_conn')
        supabase_url = conn.host
        supabase_key = conn.password
        supabase: Client = create_client(supabase_url, supabase_key)

        # Insérer les données dans supabase
        for artist in artist_clean:
            artist = {key: str(value) if value is not None else None for key, value in artist.items()}
            response = supabase.table('MAIN_ARTIST').insert(artist).execute()
            print(f"Inserting artist {artist['main_artist_name']}: {response}")

        for track in track_clean:
            track = {key: str(value) if value is not None else None for key, value in track.items()}
            response = supabase.table('TRACK').insert(track).execute()
            print(f"Inserting track {track['track_name']}: {response}")

    except Exception as e:
        print(f"Error inserting data into Supabase: {str(e)}")

# Définir le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gcs_to_supabase_dag',
    default_args=default_args,
    description='A DAG to fetch data from Spotify, save to GCS, and insert to Supabase',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 17),
    catchup=False,
)

fetch_publish_save_task = PythonOperator(
    task_id='fetch_publish_save_data',
    python_callable=fetch_publish_save_data,
    provide_context=True,
    dag=dag,
)

insert_to_supabase_task = PythonOperator(
    task_id='insert_to_supabase',
    python_callable=insert_playlists_to_supabase,
    provide_context=True,
    dag=dag,
)

fetch_publish_save_task >> insert_to_supabase_task