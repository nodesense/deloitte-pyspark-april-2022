# Databricks notebook source
# Databricks notebook source
# here general rdd apis

dayTradeSymbols = sc.parallelize(['INFY', 'MINDTREE', 'TSLA'])
swingTradeSymbols = sc.parallelize(['EMR', 'AAPL', 'TSLA', 'INFY'])


# COMMAND ----------

# return the elements/stock that are present in both
intersectionRdd = dayTradeSymbols.intersection(swingTradeSymbols)
intersectionRdd.collect()

# COMMAND ----------


# union of two RDD, add items from both the RDD, duplicates possible
unionRdd = dayTradeSymbols.union(swingTradeSymbols)
unionRdd.collect()

# COMMAND ----------

# distrinct , remove duplicates 
# unionRdd has duplicates TSLA, INFY coming twice
distinctRdd = unionRdd.distinct()
distinctRdd.collect()

# COMMAND ----------

