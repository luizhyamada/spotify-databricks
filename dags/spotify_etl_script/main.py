import json
import os
from datetime import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import logging
import boto3
from botocore.exceptions import ClientError
from typing import List

load_dotenv()

class SpotifyAuthenticator:
    def __init__(self):
        self.client_id = os.environ.get('SPOTIPY_CLIENT_ID')
        self.client_secret = os.environ.get('SPOTIPY_CLIENT_SECRET')

        if not self.client_id or not self.client_secret:
            raise ValueError("SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET environment variables are required")

        self.client_credentials_manager = SpotifyClientCredentials(
            client_id=self.client_id,
            client_secret=self.client_secret
        )

    def get_credentials_manager(self):
        return self.client_credentials_manager


class SpotifyPlaylist:
    def __init__(self, playlist_url):
        self.playlist_url = playlist_url
        self.spotipy_client = spotipy.Spotify(
            client_credentials_manager=SpotifyAuthenticator().get_credentials_manager())

    def get_playlist_info(self):
        try:
            playlist_id = self.playlist_url.split('/')[-1]
            return self.spotipy_client.playlist_items(playlist_id)
        except Exception as e:
            logging.error(f"Error getting playlist info: {e}")
            return None


class DataExtractor:
    @staticmethod
    def extract(playlist_info):
        pass


class AlbumDataExtractor(DataExtractor):
    @staticmethod
    def extract(playlist_info):
        album_data = []
        if 'items' in playlist_info:
            for item in playlist_info['items']:
                album_id = item['track']['album']['id']
                album_name = item['track']['album']['name']
                album_release_date = item['track']['album']['release_date']
                album_total_tracks = item['track']['album']['total_tracks']
                album_url = item['track']['album']['external_urls']['spotify']
                created_at = datetime.now().isoformat()
                updated_at = datetime.now().isoformat()
                album_element = {
                    'album_id': album_id,
                    'name': album_name,
                    'release_date': album_release_date,
                    'total_tracks': album_total_tracks,
                    'url': album_url,
                    'created_at': created_at,
                    'updated_at': updated_at
                }
                album_data.append(album_element)
        return album_data


class ArtistDataExtractor(DataExtractor):
    @staticmethod
    def extract(playlist_info):
        artist_data = []
        if 'items' in playlist_info:
            for data in playlist_info['items']:
                for key, value in data.items():
                    if key == "track":
                        for artist in value['artists']:
                            created_at = datetime.now().isoformat()
                            updated_at = datetime.now().isoformat()
                            artist_element = {
                                'artist_id': artist['id'],
                                'artist_name': artist['name'],
                                'external_url': artist['href'],
                                'created_at': created_at,
                                'updated_at': updated_at
                            }
                            artist_data.append(artist_element)
        return artist_data


class TrackDataExtractor(DataExtractor):
    @staticmethod
    def extract(playlist_info):
        track_data = []
        if 'items' in playlist_info:
            for item in playlist_info['items']:
                track_id = item['track']['id']
                track_name = item['track']['name']
                track_duration = item['track']['duration_ms']
                track_url = item['track']['external_urls']['spotify']
                track_popularity = item['track']['popularity']
                track_added = item['added_at']
                album_id = item['track']['album']['id']
                artist_id = item['track']['album']['artists'][0]['id']
                created_at = datetime.now().isoformat()
                updated_at = datetime.now().isoformat()
                track_element = {
                    'track_id': track_id,
                    'track_name': track_name,
                    'duration_ms': track_duration,
                    'url': track_url,
                    'popularity': track_popularity,
                    'added_at': track_added,
                    'album_id': album_id,
                    'artist_id': artist_id,
                    'created_at': created_at,
                    'updated_at': updated_at
                }
                track_data.append(track_element)
        return track_data


class DataWriter:
    def __init__(self, s3_uploader, folder_name, file_prefix):
        self.s3_uploader = s3_uploader
        self.folder_name = folder_name
        self.file_prefix = file_prefix

    def write(self, data):
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        key = f"{self.folder_name}/{self.file_prefix}_{timestamp}.json"
        data_lines = [json.dumps(entry) for entry in data]

        # Join the list of JSON strings with newlines
        data_json = '\n'.join(data_lines)

        self.s3_uploader.upload_data(data_json, key)

class S3Bucket:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ID"),
            aws_secret_access_key=os.getenv("AWS_KEY")
        )

        if not self.bucket_exists(self.bucket_name):
            self.create_bucket(self.bucket_name)

    def bucket_exists(self, bucket_name):
        try:
            self.s3.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            return False

    def create_bucket(self, bucket_name):
        try:
            self.s3.create_bucket(Bucket=bucket_name)
        except ClientError as e:
            logging.error(f"Error creating bucket '{bucket_name}': {e}")

    def upload_data(self, data, key):
        self.s3.put_object(Body=data, Bucket=self.bucket_name, Key=key)

class S3DataUploader:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_uploader = S3Bucket(bucket_name)

    def albums_upload(self, data, timestamp):
        datetime_obj = datetime.strptime(timestamp, "%Y%m%d%H%M%S")

        formatted_date = datetime_obj.strftime("date=%Y-%m-%d")

        album_data_str = '\n'.join([json.dumps(entry) for entry in data])
        album_key = f"data/albums/{formatted_date}/albums_{timestamp}.json"
        self.s3_uploader.upload_data(album_data_str, album_key)

    def artists_upload(self, data, timestamp):
        datetime_obj = datetime.strptime(timestamp, "%Y%m%d%H%M%S")

        formatted_date = datetime_obj.strftime("date=%Y-%m-%d")
        artist_key = f"data/artists/{formatted_date}/tracks_{timestamp}.json"
        artist_data_str = '\n'.join([json.dumps(entry) for entry in data])
        self.s3_uploader.upload_data(artist_data_str, artist_key)

    def songs_upload(self, data, timestamp):
        datetime_obj = datetime.strptime(timestamp, "%Y%m%d%H%M%S")

        formatted_date = datetime_obj.strftime("date=%Y-%m-%d")
        tracks_key = f"data/tracks/{formatted_date}/tracks_{timestamp}.json"
        track_data_str = '\n'.join([json.dumps(entry) for entry in data])


        self.s3_uploader.upload_data(track_data_str, tracks_key)


if __name__ == "__main__":
    PLAYLIST_URL = "https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp"

    playlist = SpotifyPlaylist(PLAYLIST_URL)
    playlist_info = playlist.get_playlist_info()

    album_data = AlbumDataExtractor.extract(playlist_info)
    artist_data = ArtistDataExtractor.extract(playlist_info)
    track_data = TrackDataExtractor.extract(playlist_info)

    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    s3_uploader = S3DataUploader(os.getenv("BUCKET_NAME"))
    s3_uploader.albums_upload(album_data, timestamp)
    s3_uploader.artists_upload(artist_data, timestamp)
    s3_uploader.songs_upload(track_data, timestamp)