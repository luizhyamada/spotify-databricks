# Databricks notebook source
df = sql("""
    with base as (
        select
            t.track_id,
            t.track_name,
            t.track_popularity,
            al.album_id,
            al.album_name,
            ar.artist_id,
            ar.artist_name,
            current_timestamp() as created_at,
            current_timestamp() as updated_at,
            current_date() as date
        from silver.tb_tracks t
        left join silver.tb_albums al on t.album_id = al.album_id
        left join silver.tb_artists ar on t.artist_id = ar.artist_id
    )
    select
        track_name,
        track_popularity,
        album_name,
        artist_name
    from
        base 
""")

display(df)

# COMMAND ----------

(
    df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold.tb_songs_agg")
)
