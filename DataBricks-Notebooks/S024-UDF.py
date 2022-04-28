# Databricks notebook source
# UDF - User Defined Functions
# useful to extend spark sql functions with custom code

power = lambda n : n * n

from pyspark.sql.functions import udf 
from pyspark.sql.types import LongType
# create udf with return type
powerUdf = udf(power, LongType())

# we must register udf in spark session
# udf too private within spark session, udf registered in spark session not avaialble in another spark session
spark.udf.register("power", powerUdf)

# COMMAND ----------

# power is udf function
spark.sql("SELECT power(5)").show()

# COMMAND ----------

