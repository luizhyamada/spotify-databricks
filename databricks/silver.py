# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # creating silver tb_album

# COMMAND ----------

bronze_path = "/mnt/bronze-luiz-spotify-project-how/data"
df_album = spark.read.format("parquet").load(bronze_path)

# COMMAND ----------

from pyspark.sql.functions import col, element_at

df_album = df_album \
    .withColumn("album_id", col("track.album.id")) \
    .withColumn("album_name", col("track.album.name")) \
    .withColumn("album_release_date", col("track.album.release_date")) \
    .withColumn("album_total_tracks", col("track.album.total_tracks")) \
    .select("album_id", "album_name", "album_release_date", "album_total_tracks", "created_at", "updated_at", "date")  

df_album = df_album.dropDuplicates(subset=["album_id"])

display(df_album)

# COMMAND ----------

(
    df_album
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.tb_albums")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # creating silver tb_artist

# COMMAND ----------

bronze_path = "/mnt/bronze-luiz-spotify-project-how/data"
df_artist = spark.read.format("parquet").load(bronze_path)

df_artist = df_artist \
    .withColumn("artist_id", element_at(col("track.album.artists.id"), 1))\
    .withColumn("artist_name", element_at(col("track.album.artists.name"), 1))\
    .select("artist_id", "artist_name", "created_at", "updated_at", "date") 

df_artist = df_artist.dropDuplicates(subset=["artist_id"])

display(df_artist)

# COMMAND ----------

(
    df_artist
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.tb_artists")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # creating silver tb_tracks

# COMMAND ----------

bronze_path = "/mnt/bronze-luiz-spotify-project-how/data"
df_track = spark.read.format("parquet").load(bronze_path)

df_track = df_track \
    .withColumn("track_id", col("track.id"))\
    .withColumn("album_id", col("track.album.id"))\
    .withColumn("artist_id", element_at(col("track.album.artists.id"), 1))\
    .withColumn("track_name", col("track.name"))\
    .withColumn("track_popularity", col("track.popularity"))\
    .withColumn("track_added_at", col("added_at"))\
    .select("track_id", "album_id", "artist_id", "track_name", "track_popularity", "track_added_at", "created_at", "updated_at", "date")

display(df_track)

# COMMAND ----------

(
    df_track
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.tb_tracks")
)
