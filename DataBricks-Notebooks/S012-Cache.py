# Databricks notebook source
# /FileStore/tables/books/book_war_and_peace.txt

textFileRdd = sc.textFile("/FileStore/tables/books/book_war_and_peace.txt")
print("partititons ", textFileRdd.getNumPartitions()) # sc.defaultMinPartitions

print("Count", textFileRdd.count()) # lines count

print ("After repartition")
textFileRdd = textFileRdd.repartition(16)
print("textFileRdd ", textFileRdd.getNumPartitions()) # sc.defaultMinPartitions
print("Count", textFileRdd.count()) # lines count


# COMMAND ----------

# take n records
textFileRdd.take(5)

# COMMAND ----------

# first line
textFileRdd.first()

# COMMAND ----------

# how to remove special chars
# we will have only all ascii
def to_ascii(text):
    import re
    output = re.sub(r"[^a-zA-Z0-9 ]","",text)
    #print(output)
    return output

text = "prince, don't ;welcome"
to_ascii(text)

# COMMAND ----------

wordCountRdd = textFileRdd\
                 .map (lambda line: to_ascii(line))\
                  .map (lambda line: line.strip().lower())\
                 .map (lambda line: line.split(" "))\
                 .flatMap(lambda elements: elements)\
                 .filter (lambda word: word != "")\
                 .map (lambda word: (word, 1))\
                 .reduceByKey(lambda acc, value: acc + value)

from pyspark import StorageLevel

# both persist/cache are lazy evaluated, at least one operation to be performed before for caching
#wordCountRdd.cache () # this function calls persist method with (MEMORY_ONLY option)
# wordCountRdd.persist(StorageLevel.MEMORY_ONLY) # same like cache
# wordCountRdd.persist(StorageLevel.DISK_ONLY) 
wordCountRdd.persist(StorageLevel.MEMORY_AND_DISK) 

# cache - a place in memory where spark output is cached
# why cache?
#   Save too much of computing time
#   Save the amount of data loading from IO/S3/HDFS/Azure tc
#  When  to cache?
#    Whenever RDD/DataFrame is reused again and again, which will process data again and again, load data again and again
#    using cache for computed result avoid re-computation and re-read from ios

# Cache is stored in executor memory, not in DRIVER

# Cache can be stored in MEMORY at executor
# Cache can be stored in DISK at executor
# CAche can be stored in MEMORY AND ALSO DISK if not enough memory to store cache in RAM

# REplication for Cache
#  Cache can be replicated with maximum 2 executors [2 executors will have same copy of cache]


# COMMAND ----------

wordCountRdd.is_cached

# COMMAND ----------

wordCountRdd.take(5)

# COMMAND ----------

wordCountRdd.getNumPartitions()

# COMMAND ----------

#wordCountPair =  ('like', 734), a python tuple pair of elemnts 
# wordCountPair[0] means 'like'
# wordCountPair[1] means 734
wordCountRddAscending = wordCountRdd.sortBy(lambda wordCountPair: wordCountPair[1])
wordCountRddAscending.take(20)

# COMMAND ----------

# reusing RDD [2] for soring in decending order
sortedRddDecending = wordCountRdd.sortBy(lambda kv: kv[1], ascending=False)
sortedRddDecending.take(20)

# COMMAND ----------

# we have 16 partitions, 16 files shall be created
wordCountRdd.saveAsTextFile("/FileStore/tables/2022-4-25/wordcount-all2")

# COMMAND ----------

# we have 16 partitions,  coalesce into 1 partition, then writing single file
wordCountRdd.coalesce(1)\
            .saveAsTextFile("/FileStore/tables/2022-4-25/wordcount-single2")

# COMMAND ----------

