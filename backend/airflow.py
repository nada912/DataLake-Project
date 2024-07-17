import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from supabase import create_client, Client

def fetch_spotify_data():
    client_id = 'a7520394dac34834a7330006d163693d'
    client_secret = 'de9a269ca89e40c7b2b542ee51d68b97'
    auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(auth_manager=auth_manager)
    results = sp.search(q='genre:pop', type='artist', limit=10)
    artists = results['artists']['items']
    artist_data = [{'main_artist_id': str(artist['id']), 'main_artist_name': str(artist['name']),
                    'main_artist_popularity': str(artist['popularity']), 'main_artist_genre': str(','.join(artist['genres'])),
                    'main_artist_image_url': str(artist['images'][0]['url']) if artist['images'] else None} for artist in artists]
    
    print("Fetched artists data:", artist_data)
    return json.dumps(artist_data)

def insert_data_to_supabase(artists_json):
    artist_data = json.loads(artists_json)
    supabase_url = 'https://ufxjgdplumapkpmxtaat.supabase.co'
    supabase_key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVmeGpnZHBsdW1hcGtwbXh0YWF0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3MjEwMjcyNDEsImV4cCI6MjAzNjYwMzI0MX0.BZfh_KBPtXyDFJ2BzXyRbAazz-f1BhzfKcqA0spVXv8'
    supabase: Client = create_client(supabase_url, supabase_key)
    
    for artist in artist_data:
        artist = {key: str(value) if value is not None else None for key, value in artist.items()}
        response = supabase.table('MAIN_ARTIST').insert(artist).execute()
        print(f"Inserting artist {artist['main_artist_name']}: {response}")

artists_json = fetch_spotify_data()
insert_data_to_supabase(artists_json)
