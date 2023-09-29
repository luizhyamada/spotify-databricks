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
# MAGIC CREATE TABLE IF NOT EXISTS silver.tb_albums
# MAGIC (
# MAGIC   album_id string,
# MAGIC   album_name string,
# MAGIC   album_release_date string,
# MAGIC   album_total_tracks long,
# MAGIC   created_at string,
# MAGIC   updated_at string,
# MAGIC   date date
# MAGIC )
# MAGIC LOCATION '/mnt/silver-luiz-spotify-project-how/data/albums/';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.tb_artists
# MAGIC (
# MAGIC   artist_id string,
# MAGIC   artist_name string,
# MAGIC   created_at string,
# MAGIC   updated_at string,
# MAGIC   date date
# MAGIC )
# MAGIC LOCATION '/mnt/silver-luiz-spotify-project-how/data/artists/';

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver.tb_tracks
# MAGIC (
# MAGIC   track_id string,
# MAGIC   album_id string,
# MAGIC   artist_id string,
# MAGIC   track_name string,
# MAGIC   track_popularity long,
# MAGIC   track_added_at string,
# MAGIC   created_at string,
# MAGIC   updated_at string,
# MAGIC   date date
# MAGIC )
# MAGIC LOCATION '/mnt/silver-luiz-spotify-project-how/data/tracks/';

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Creating tables gold

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold.tb_songs_agg;
# MAGIC CREATE TABLE IF NOT EXISTS gold.tb_songs_agg
# MAGIC (
# MAGIC   track_name string,
# MAGIC   track_popularity long,
# MAGIC   album_name string,
# MAGIC   artist_name string
# MAGIC )
# MAGIC
# MAGIC LOCATION '/mnt/gold-luiz-spotify-project-how/data/songs';
