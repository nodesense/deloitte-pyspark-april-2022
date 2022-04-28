# Databricks notebook source
# map - take   element/input and process and return result
# flatMap - flatten the return value as element

# celcious to fahrenheit
data = [-7, 0, 4, 10]
celciousRdd = sc.parallelize(data)
fahrenheitRdd = celciousRdd.map (lambda cel: (cel * 1.8) + 32)

fahrenheitRdd.collect()

# COMMAND ----------

# flatMap - convert list into element, flatten list of list
data = [
  [1,2,5,6],
  [3, 4],
  [7,8,9]
]

# line contiuation \
mapOutput = sc.parallelize(data)\
               .map(lambda l: l)

print("mapoutput", mapOutput.collect())
              


# COMMAND ----------

# flatmap, project elment in the list as separate element
flatmapOutput = sc.parallelize(data)\
               .flatMap(lambda l: l)

print("flatmapOutput", flatmapOutput.collect())

# COMMAND ----------

lines = [
  "Python SCALA pyspark databricsk",
  "python sCala pyspark aws"
]
# get a tuple with (word, 1), (python, 1) (scala, 1)

wordPairRdd = sc.parallelize(lines)\
                 .map (lambda line: line.split(' '))\
                 .flatMap (lambda wordArray: wordArray)\
                 .map (lambda word: word.lower())\
                 .map (lambda word: (word, 1) )\
                 .reduceByKey(lambda acc, value: acc + value)

wordPairRdd.collect()

# COMMAND ----------

