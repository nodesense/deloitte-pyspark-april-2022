# Databricks notebook source
# accumulator designed to fetch data from executor to notebook
# Databricks notebook source
# accumulator useful to collect data from executor to driver program
rdd = sc.parallelize(range(0, 20))

sumAccum = sc.accumulator(0)

rdd.foreach(lambda n: sumAccum.add(n))  # run inside executor

print("Acc value is ", sumAccum.value) # driver

# COMMAND ----------


sumFirstValueInPartitionAccum = sc.accumulator(0)

# write a accumulator that picks first element in each partition and sum them up
def sumFirstElement(partitionItr):
  global sumFirstValueInPartitionAccum
  for n in partitionItr: 
    sumFirstValueInPartitionAccum.add(n)
    break # we process only 1st element in the partition and end the loop
    
rdd.foreachPartition(lambda itr: sumFirstElement(itr))

print ("sum of first elements ", sumFirstValueInPartitionAccum.value)


# COMMAND ----------


# custom accumulator
# collect first element in each parition [not to sum/count]
# zero, addInPlace are default functions will be invoked by accumulator 
from  pyspark.accumulators import AccumulatorParam
class ListItemParamAccumulator(AccumulatorParam):
  def zero(self, v):
    return [] # return list, empty list used when no seed value given initially
  # variable is list, value is the arg we pass via add function
  # acc.add(value) ==> calls addInPlace()
  def addInPlace(self, variable, value):
    variable.append(value)
    return variable
  

# COMMAND ----------

# create accumulator
# [] is empty list
# ListItemParamAccumulator is custom accumulator defiend above
firstValueAccum = sc.accumulator([], ListItemParamAccumulator())


# write a accumulator that picks first element in each partition and sum them up
def sampleFirstElement(partitionItr):
  global firstValueAccum
  for n in partitionItr: 
    # n is passed as value to addInPlace function
    firstValueAccum.add(n) # this will call ListItemParamAccumulator addInPlace function
    break
    
rdd.foreachPartition(lambda itr: sampleFirstElement(itr))

print (" first elements in each partition ", firstValueAccum.value)


# COMMAND ----------

rdd.glom().collect()

# COMMAND ----------

