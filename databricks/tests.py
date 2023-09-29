# Databricks notebook source
dbutils.fs.rm('dbfs:/mnt/gold-luiz-spotify-project-how/', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.tb_spotify

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.tb_tracks

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.tb_artists

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.tb_albums

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe bronze.tb_albums

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe silver.tb_albums

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *
# MAGIC from silver.top_50_vw

# COMMAND ----------

dbutils.fs.ls("mnt/")
