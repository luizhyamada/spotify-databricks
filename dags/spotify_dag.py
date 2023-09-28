from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from spotify_etl_script.main import SpotifyPlaylist, DataWriter


with DAG ('spotify_dag', start_date = datetime(2023, 9, 27),
          schedule_interval=None, catchup= False) as dag:

    def extract_spotify_data():
        PLAYLIST_URL = "https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp"

        playlist = SpotifyPlaylist(PLAYLIST_URL)
        data = playlist.get_playlist_info()

        writer = DataWriter()
        writer.write(data)

    extract_task = PythonOperator(
        task_id='extract_spotify_data',
        python_callable=extract_spotify_data,
        dag=dag,
    )

# Define task dependencies
extract_task