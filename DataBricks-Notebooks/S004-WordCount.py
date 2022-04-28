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
wordCountRdd.saveAsTextFile("/FileStore/tables/2022-4-25/wordcount-all")

# COMMAND ----------

# we have 16 partitions,  coalesce into 1 partition, then writing single file
wordCountRdd.coalesce(1)\
            .saveAsTextFile("/FileStore/tables/2022-4-25/wordcount-single")

# COMMAND ----------

