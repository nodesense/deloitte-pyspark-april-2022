import os



SCALA_VERSION = '2.11'
SPARK_VERSION = '2.4.7'

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'


import findspark
findspark.init()

import pyspark

# consume from a topic called invoicess
# calculate aggregate , print data on console
# publish the aggregated values back to kafka as JSON

# kafka create a topic called "aggregated-invoices"

# kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic aggregated-invoices

# run consumer to listen on messages from aggregated-invoices
# kafka-console-consumer --bootstrap-server localhost:9092 --topic aggregated-invoices 


from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]")\
                            .appName("SparkWordCountStreamKafka").getOrCreate()


# read from kafka, here spark is consumer for kafka topic called invoices
# spark streaming works as dataframe/sql
kafkaDf = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "words")\
  .load()

# .show/print will not work directily due to stream..
# linesDf.show() # worn't work
kafkaDf.printSchema() # works

# key is kafka key, in binary format
# value is kafka value, in binary format
# topic string
# parition, integer
# offer long 
# timestamp - longint in ms
# timestampType - Source Time, Record write time

# now convert kafka value which is in bytes to STRING, we ignore the key for now...
# now we pick only value from the stream..
linesDf = kafkaDf.selectExpr("CAST(value AS STRING)")
linesDf.printSchema() # we get only value as string

# split the lines into words, then convert the words into individual row using a function called explode
# explode will convert columns/array elements into spark record
import pyspark.sql.functions as F
# linesDf.value is a column
# split convert to list of words [welcome, to, spark]
# convert list of words into individual word/record
# explode, will convert elements into record
#wordsDf = linesDf.select(F.split(linesDf.value," "))
# after explode the output would be, column name is shown as col
#        welcome
#         to
#         spark
# wordsDf = linesDf.select(F.explode(F.split(linesDf.value," ")) )

wordsDf = linesDf.select(F.explode(F.split(linesDf.value," ")).alias("word") )
#        welcome
#         to
#         spark
# now the same result with col name word

# generate running word count from stream
# "word" is a column name
wordCountsDf = wordsDf.groupBy("word").count()

# to print the data on console..
# read the data send by nc command from linux terminal, print it on Jupyter console
echoOnconsole = wordCountsDf\
                .writeStream\
                .outputMode("complete")\
                .format("console")\
                .start() # start the query. spark will subscribe for data

echoOnconsole.awaitTermination()

# later you can terminal the jupyter
