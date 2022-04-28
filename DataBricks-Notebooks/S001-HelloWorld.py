# Databricks notebook source
# this application/notebook is known as spark application
# run on spark driver, not on distributed systems
print ("Hello")

# COMMAND ----------

# the last evaluated statement/results shall be printed
10 + 3 # run on driver

# COMMAND ----------

# Pyton list of strings
data = ["SPARK", "Kafka", "ApaCHE", "Akka", "lua"] # 

# create RDD from in memory data/hard coded data
# create rdd using existing data, lazy evaluation
rdd = sc.parallelize (data)

# the lambda function called wiht map - shall be executed inside executor
lowercaseRdd = rdd.map (lambda word: word.lower()) # TRANSFORMATION, LAZY EVALUATION

# ACTION - Execution, job, task, task rusn on executors, results collected
output = lowercaseRdd.collect()

print(output) # run on driver

# COMMAND ----------

# on lower case rdd, create a paired rdd (key, value) usig Python tuple ()
# we reuse lowercaseRdd as parent - lineage

wordLengthRdd = lowercaseRdd.map (lambda word: (word, len(word)) )

wordLengthRdd.collect() 