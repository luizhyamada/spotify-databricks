# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Create bronze table albums

# COMMAND ----------

raw_path = "/mnt/raw-luiz-spotify-project-how/data/albums/"
df_album = spark.read.json(raw_path)
display(df_album)

# COMMAND ----------

(
    df_album
    .write
    .partitionBy("date")
    .mode("overwrite")
    .format("delta")
    .saveAsTable("bronze.tb_albums")
)

# COMMAND ----------

df_album.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create bronze table artists

# COMMAND ----------

raw_path = "/mnt/raw-luiz-spotify-project-how/data/artists/"
df_artists = spark.read.json(raw_path)
display(df_artists)

# COMMAND ----------

(
    df_artists
    .write
    .partitionBy("date")
    .mode("overwrite")
    .format("delta")
    .saveAsTable("bronze.tb_artists")
)

# COMMAND ----------

df_artists.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Create bronze table songs

# COMMAND ----------

raw_path = "/mnt/raw-luiz-spotify-project-how/data/tracks/"
df_tracks = spark.read.json(raw_path)
display(df_tracks)

# COMMAND ----------

(
    df_tracks
    .write
    .partitionBy("date")
    .mode("overwrite")
    .format("delta")
    .saveAsTable("bronze.tb_tracks")
)

# COMMAND ----------


