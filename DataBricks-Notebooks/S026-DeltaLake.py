# Databricks notebook source

# Databricks notebook source
def showJson(path):
  import json
  contents = sc.textFile(path).collect()
  for content in contents:
    result = json.loads(content)
    print(json.dumps(result, indent = 3))

# COMMAND ----------

dbutils.fs.rm("/user/hive/warehouse/branddb.db", True)
spark.sql("DROP DATABASE IF EXISTS branddb")

# COMMAND ----------


spark.sql ("CREATE DATABASE branddb")

# COMMAND ----------


# Delta lake shall have meta data about table, columns, data types etc who created 
spark.sql ("CREATE TABLE branddb.brands (id INT, name STRING)")


# COMMAND ----------


showJson("/user/hive/warehouse/branddb.db/brands/_delta_log/00000000000000000000.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- this will create a parquet file inside brands directory, 
# MAGIC -- the new parquet file shall be added into new json file, placed under delta logs 0000000xxx1.json
# MAGIC 
# MAGIC INSERT INTO branddb.brands VALUES(1, 'Apple')

# COMMAND ----------

showJson("/user/hive/warehouse/branddb.db/brands/_delta_log/00000000000000000001.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- this will create a parquet file inside brands directory
# MAGIC -- the new parquet file shall be added into new json file, placed under delta logs 0000000xxx1.json
# MAGIC INSERT INTO branddb.brands VALUES(2, 'Google')

# COMMAND ----------

showJson("/user/hive/warehouse/branddb.db/brands/_delta_log/00000000000000000002.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- spark queries uses json files locatd in _delta_logs folder/directory
# MAGIC -- spark tally add/remove parquet fiels in json
# MAGIC -- then query the data from applicable files [not in removed files]
# MAGIC SELECT * FROM branddb.brands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update
# MAGIC -- delta log, the existing file that contains Google record shall be removed, not physically in the _delta_logs
# MAGIC -- new file with updated value Alphabet shall be added in to data directory and also _delta_logs
# MAGIC UPDATE branddb.brands set name='Alphabet' where id=2

# COMMAND ----------

showJson("/user/hive/warehouse/branddb.db/brands/_delta_log/00000000000000000003.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM branddb.brands

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM branddb.brands where id = 1

# COMMAND ----------

showJson("/user/hive/warehouse/branddb.db/brands/_delta_log/00000000000000000004.json")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM branddb.brands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- time travel, Rollback to previous version, developer realized that removable of appled id 1 by mistake
# MAGIC 
# MAGIC INSERT INTO branddb.brands SELECT * from branddb.brands VERSION AS OF 2 WHERE id = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM branddb.brands

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from branddb.brands VERSION AS OF 2 WHERE id = 1

# COMMAND ----------

