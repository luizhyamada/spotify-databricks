# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # creating left join table

# COMMAND ----------

df = sql("""
    select distinct
        t.track_id,
        t.track_name,
        t.duration_ms,
        t.popularity,
        al.album_id,
        al.name as album_name,
        ar.artist_name,
        current_timestamp() as created_at,
        current_timestamp() as updated_at,
        current_date() as date_partition
    from bronze.tb_tracks t
    left join bronze.tb_albums al on t.album_id = al.album_id
    left join bronze.tb_artists ar on t.artist_id = ar.artist_id
""")

display(df)

# COMMAND ----------

(
    df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.tb_tracks")
)
