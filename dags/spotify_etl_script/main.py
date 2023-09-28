import json
import os
from io import BytesIO
from os import getenv
import datetime
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv
import logging
import boto3
from botocore.exceptions import ClientError
from typing import List

load_dotenv()
PLAYLIST_URL ="https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp"

class SpotifyAuthenticator:
    def __init__(self):
        self.client_id = getenv('SPOTIPY_CLIENT_ID')
        self.client_secret = getenv('SPOTIPY_CLIENT_SECRET')

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
            response = self.spotipy_client.playlist_items(playlist_id)
            if 'items' in response:
                return response['items']
        except Exception as e:
            logging.error(f"Error getting playlist info: {e}")
            return None
class AWSCredentials:
    def __init__(self):
        self.aws_access_key = getenv('AWS_ID')
        self.aws_secret_key = getenv('AWS_KEY')

        if not self.aws_access_key or not self.aws_secret_key:
            raise ValueError("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables are required")

class DataWriter:
    def __init__(self):
        self.data_buffer = BytesIO()  # Use BytesIO to store data in memory
        aws_credentials = AWSCredentials()
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_credentials.aws_access_key,
            aws_secret_access_key=aws_credentials.aws_secret_key,
        )

    def _write_row(self, row: str):
        self.data_buffer.write(row.encode())

    def _generate_key(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        return f"data/extract_at={now.date()}/spotify_{now}.json"

    def _write_to_file(self, data: [List, dict]):
        if isinstance(data, dict):
            self._write_row(json.dumps(data) + "\n")
        elif isinstance(data, list):
            for element in data:
                self._write_row(json.dumps(element) + "\n")
        else:
            raise ValueError

    def write(self, data: [List, dict]):
        self.key = self._generate_key()
        self._write_to_file(data=data)
        self._write_to_s3()

    def _write_to_s3(self):
        self.data_buffer.seek(0)  # Reset the buffer position to the beginning
        self.client.put_object(
            Body=self.data_buffer,
            Bucket="raw-luiz-spotify-project-how",
            Key=self.key
        )

if __name__ == '__main__':
    playlist = SpotifyPlaylist(PLAYLIST_URL)
    data = playlist.get_playlist_info()

    writer = DataWriter()
    writer.write(data)


