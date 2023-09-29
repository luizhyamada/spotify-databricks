# Databricks notebook source
raw_path = "/mnt/raw-luiz-spotify-project-how/data"
df = spark.read.json(raw_path)
display(df)

# COMMAND ----------

bronze_path = "/mnt/bronze-luiz-spotify-project-how/data"
(
    df.write.partitionBy("date").mode("overwrite").parquet(bronze_path)
)
