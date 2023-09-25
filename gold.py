# Databricks notebook source
df = sql("""
    select
        track_name,
        artist_name,
        avg(duration_ms) as avg_duration,
        avg(popularity) as avg_popularity
    from silver.tb_tracks
    group by track_name, artist_name
    order by artist_name desc
""")



# COMMAND ----------

(
    df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold.tb_tracks_details_agg")
)

# COMMAND ----------

df = sql("""
    select
        album_name,
        count(*) as total_tracks
    from silver.tb_tracks
    group by album_name
    order by total_tracks desc
""")

# COMMAND ----------

(
    df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold.tb_albums_details_agg")
)

# COMMAND ----------

df = sql("""
    select 
        artist_name, 
        count(*) as artist_appearances
    from silver.tb_tracks
    group by artist_name
    order by artist_appearances desc;
""")


# COMMAND ----------

(
    df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold.tb_artists_details_agg")
)
