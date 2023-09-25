# Spotify - Databricks
## Introduction
This is an ETL project using the Spotify API to get the daily top 50 US songs in the app. 

## About Dataset/API
Spotipy is a lightweight Python library for the Spotify Web API. With Spotipy you get full access to all of the music data provided by the Spotify platform. Wit this API, data will be extracted to get "artist", "album" and "song" data. You can check the [Spotipy API](https://spotipy.readthedocs.io/) for more information and documentation regarding the API.

## Tools
- S3 (Simple Storage Service): Amazon S3 is a highly scalable object storage service that can store an retrieve any amount of data from anywhere on the web. It is commonly used to store and distribute large media files, data backups and static website files.
- Databricks: Databricks is a unified analytics platform that is designed to simplify the process of building big data. It provides a collaborative environment for data engineering, data science, and machine learning that enables organizations to process and analyze large datasets efficiently.
- Airflow: Apache Airflow is an open-source platform designed for orchestrating, scheduling, and monitoring complex data workflows and data pipeline automation. It allows you to define, schedule, and manage workflows as Directed Acyclic Graphs (DAGs) of tasks.

## Architecture

![image](https://github.com/luizhyamada/spotify-databrics/assets/57925185/4eff1d7e-a5fc-408c-9c10-f0d8bb0e3019)

## Steps
- Use the Spotipy API to extract data from Spotify.
- Load raw data into a S3 Bucket.
- Use Databricks to process the data.
- Use Airflow to schedule the data extraction and load.
