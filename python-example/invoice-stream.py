import os

SCALA_VERSION = '2.11'
SPARK_VERSION = '2.4.7'

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION} pyspark-shell'


import findspark
findspark.init()

import pyspark


from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]")\
                            .appName("SparkStreamingKafkaBasic").getOrCreate()


# read from kafka, here spark is consumer for kafka topic called invoices
# spark streaming works as dataframe/sql
kafkaDf = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "invoices2")\
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
invoiceJsonRawDf = kafkaDf.selectExpr("CAST(value AS STRING)")
invoiceJsonRawDf.printSchema() # we get only value as string

import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType
# json is object, spark DF needs schema 

schema = StructType(
        [
            StructField("InvoiceNo", IntegerType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("Description", StringType(), True),
            StructField("InvoiceDate", StringType(), True),
            #StructField("InvoiceDate", DateType(), True),
            StructField("UnitPrice", DoubleType(), True),
            StructField("CustomerID", IntegerType(), True),
            StructField("Country", StringType(), True),
        ]
)


#{"InvoiceNo": 495774, "StockCode": "84406G", "Quantity": 2, "Description": "TODO", "InvoiceDate": "05/22/2021 00:36", "UnitPrice": 2.0, "CustomerID": 17850, "Country": "AT"}

# replacing json string with a json object with schema
# now value is a column, it contains a struct
jsonDf = invoiceJsonRawDf.withColumn("value", F.from_json("value", schema))

# now we will extract value which struct type ewith all schema field mention, to specific columns
#InvoiceNo, StockCode, ....
invoiceDf = jsonDf.select(F.col("value.*"))


# to print the data on console..
# read the data send by nc command from linux terminal, print it on Jupyter console
echoOnconsole = invoiceDf\
                .writeStream\
                .outputMode("append")\
                .format("console")\
                .start() # start the query. spark will subscribe for data

echoOnconsole.awaitTermination()

# later you can terminal the jupyter