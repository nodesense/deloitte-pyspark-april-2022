# Databricks notebook source
data = range(1, 65) # 1 to 50

# every rdd has partitions where data is stored as chunk
# use parallelize to load in memory data into rdd
numbersRdd = sc.parallelize(data)

# COMMAND ----------

# get number of partitions


numbersRdd.getNumPartitions()

# COMMAND ----------

# collect - action method, execution
# get all teh data from all the partitions, finally merge all data into one list and return the list
# don't use collect method as it collect data from multiple exeuctors to driver /this application
# memory may be insufficient
result = numbersRdd.collect()
print(result)

# COMMAND ----------

# glom....collect()
# collect data from partition , return them as a list, finally list of list made
result = numbersRdd.glom().collect()
result

# COMMAND ----------

# spark has many default settings, which can be configured/overwritten
# defaultParallism - used in parallize method, how many tasks can be executed at time
# minPartititons - used by readText, or hdfs drivers as default minPartitions

# number of v.core - 2 
# parallel tasks created based on number of v.cores with factor of 1 t 4
# high CPU, heavy computing - ML - parallel tasks - factors 1 to 2
# analytical work, factors 2 to 4

# 2 cpu - 4 parallel task
# 2 cpu - 8 parallel tasks [data bricks default config]

# Spark Context -sc 
print("min partitions ", sc.defaultMinPartitions)
print ("default parallism", sc.defaultParallelism)



# COMMAND ----------

numbersRdd = sc.parallelize(data, 4)
print(numbersRdd.getNumPartitions())
numbersRdd.glom().collect()

# COMMAND ----------

# programatically spark allow to incrase partitions or decrease partitions
numbersRdd = sc.parallelize(data, 1)
print("before", numbersRdd.getNumPartitions())
# increase partitions, distribute data across executors
# repartition
rdd2 = numbersRdd.repartition(8)
print("after ", rdd2.getNumPartitions())


# COMMAND ----------

# 1 partition data
numbersRdd.glom().collect()

# COMMAND ----------

# rdd2, 8 partitions
rdd2.glom().collect()

# COMMAND ----------

rdd = sc.parallelize(range(1, 51), 8)
# reduce number of partitions
# coalesce to reduce number of partitions
# coalesce - try not to shuffle, best attempts to reduce partitiosn without shuffling
rdd2 = rdd.coalesce(2)
 
print ("rdd partitions ", rdd.getNumPartitions())
print ("rdd2 partitions ", rdd2.getNumPartitions())


# COMMAND ----------

