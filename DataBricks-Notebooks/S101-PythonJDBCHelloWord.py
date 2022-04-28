# Databricks notebook source
# Databricks notebook source
# ensure that jdbc already initiazlied in other notebook in spark
# ensure that in Scala notebook, we have created db, table test
dataframe = spark.read.format("jdbc")\
                      .option("url", "jdbc:derby:memory:myDB;create=true")\
                      .option("driver", "org.apache.derby.jdbc.EmbeddedDriver")\
                      .option("dbtable", "test")\
                      .load()
            
dataframe.show()

# COMMAND ----------


spark.createDataFrame(data=[(10,),(11,)], schema=["id"])\
.write\
 .mode("append")\
.format("jdbc")\
.option("url", "jdbc:derby:memory:myDB;create=true")\
.option("driver", "org.apache.derby.jdbc.EmbeddedDriver")\
.option("dbtable", "test")\
 .save()


# COMMAND ----------


dataframe.show()

# COMMAND ----------


dataframe.createOrReplaceTempView("test")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM test

# COMMAND ----------

