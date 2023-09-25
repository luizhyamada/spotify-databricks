# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Mounting buckets

# COMMAND ----------

def mount_s3_bucket(access_key, secret_key, aws_bucket_name):
    '''
    Create an mount point from S3 bucket.

            Parameters:
                    access_key (str): Access key id
                    secret_key (str): Secret key
                    aws_bucket_name (str): Bucket name

            Returns:
                    ret (DataFrame): Spark DataFrame with the result
    '''
    access_key = access_key
    secret_key = secret_key.replace("/", "%2F")
    ret = dbutils.fs.mount(f"s3a://{access_key}:{secret_key}@{aws_bucket_name}", f"/mnt/{aws_bucket_name}")
    return ret


# COMMAND ----------

access_key = ""
secret_key = ""
aws_bucket_name_raw = "raw-luiz-spotify-project-how"
aws_bucket_name_bronze = "bronze-luiz-spotify-project-how"
aws_bucket_name_silver = "silver-luiz-spotify-project-how"
aws_bucket_name_gold = "gold-luiz-spotify-project-how"

# COMMAND ----------

mount_s3_bucket(access_key, secret_key, aws_bucket_name_raw)
mount_s3_bucket(access_key, secret_key, aws_bucket_name_bronze)
mount_s3_bucket(access_key, secret_key, aws_bucket_name_silver)
mount_s3_bucket(access_key, secret_key, aws_bucket_name_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Creating schemas
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Creating tables silver

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.tb_albums
# MAGIC (
# MAGIC   album_id string,
# MAGIC   created_at string,
# MAGIC   name string,
# MAGIC   release_date string,
# MAGIC   total_tracks long,
# MAGIC   updated_at string,
# MAGIC   url string,
# MAGIC   date date
# MAGIC )
# MAGIC LOCATION '/mnt/bronze-luiz-spotify-project-how/data/albums/';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.tb_artists
# MAGIC (
# MAGIC   artist_id string,
# MAGIC   artist_name string,
# MAGIC   created_at string,
# MAGIC   external_url string,
# MAGIC   updated_at string,
# MAGIC   date date
# MAGIC )
# MAGIC LOCATION '/mnt/bronze-luiz-spotify-project-how/data/artists/';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze.tb_songs
# MAGIC (
# MAGIC   artist_id string,
# MAGIC   artist_name string,
# MAGIC   created_at string,
# MAGIC   external_url string,
# MAGIC   updated_at string,
# MAGIC   date date
# MAGIC )
# MAGIC LOCATION '/mnt/bronze-luiz-spotify-project-how/data/tracks/';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.tb_tracks
# MAGIC (
# MAGIC   track_id string,
# MAGIC   track_name string,
# MAGIC   duration_ms long,
# MAGIC   popularity long,
# MAGIC   album_id string,
# MAGIC   album_name string,
# MAGIC   artist_name string,
# MAGIC   created_at timestamp,
# MAGIC   updated_at timestamp,
# MAGIC   date_partition date
# MAGIC )
# MAGIC LOCATION '/mnt/silver-luiz-spotify-project-how/data/tb_tracks/';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Creating tables gold

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC drop table gold.tb_albums_details_agg

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS gold.tb_tracks_details_agg;
# MAGIC CREATE TABLE IF NOT EXISTS gold.tb_tracks_details_agg
# MAGIC (
# MAGIC track_name string,
# MAGIC artist_name string,
# MAGIC avg_duration double,
# MAGIC avg_popularity double
# MAGIC )
# MAGIC LOCATION '/mnt/gold-luiz-spotify-project-how/data/tb_tracks_details_agg/';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS gold.tb_albums_details_agg;
# MAGIC CREATE TABLE IF NOT EXISTS gold.tb_albums_details_agg
# MAGIC (
# MAGIC album_name string,
# MAGIC total_tracks long
# MAGIC )
# MAGIC LOCATION '/mnt/gold-luiz-spotify-project-how/data/tb_albums_details_agg/';

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC DROP TABLE IF EXISTS gold.tb_artists_details_agg;
# MAGIC CREATE TABLE IF NOT EXISTS gold.tb_artists_details_agg
# MAGIC (
# MAGIC artist_name string,
# MAGIC artist_appearances long
# MAGIC )
# MAGIC LOCATION '/mnt/gold-luiz-spotify-project-how/data/tb_artists_details_agg/';
# MAGIC
