# Databricks notebook source
# Databricks notebook source
# Spark Session, entry point for Spark SQL, DataFrame
# in any spark application, there will be ONLY ONE SPARK CONTEXT (sc)
# can be many spark session

spark 

# COMMAND ----------


# Spark SQL - Structured Data
# Structured Data = (Schema + data rows)
# Schema = (column, data types), meta data about data
# Data Rows = RDD data Row(field=value, ....)
# Spark SQL/DataFrame are APIs to the same data rows


# COMMAND ----------


products = [
    # (product_id, product_name, price, brand_id, offer)
    (1, 'iPhone', 1000.0, 100, 0),
    (2, 'Galaxy', 545.50, 101, None),
    (3, 'Pixel', 645.99, 101, None)
]

# no data type mentioned, however we will let spark to infer schema by reading data
schema = ['product_id', 'product_name', 'price', 'brand_id', 'offer']
# use spark session to create dataframe
productDf = spark.createDataFrame(data=products, schema=schema)

# every data frame has schema, we can print it
productDf.printSchema()
# ASCII FORMAT
productDf.show() # 20 records

# COMMAND ----------


productDf.count()


# COMMAND ----------


productDf.schema # schema attached to dataframe

# COMMAND ----------


productDf.columns

# COMMAND ----------

productDf.rdd # rdd behind dataframe


# COMMAND ----------

productDf.rdd.take(3)

# COMMAND ----------



productDf.rdd.getNumPartitions()

# COMMAND ----------


# data frame has transformation and actions
# transformations shall return dataframe which immutable
# transformation are lazy
# data frame filter
# return a new data dataframe, it won't execute the data, no job, no action
df = productDf.filter (productDf["price"] <= 750)

df.printSchema()
df.show()

# COMMAND ----------


# select api, projection 
df = productDf.select("product_name", "price")
df.printSchema()
df.show()

# COMMAND ----------


# selectExpr dynamic expression, CAST, 
# SELECT upper(product_name), price * 0.9 
df = productDf.selectExpr("product_name", "upper(product_name)", 
                          "price", "price  * .9")

df.printSchema()
df.show()


# COMMAND ----------


# selectExpr dynamic expression, CAST, 
# SELECT upper(product_name), price * 0.9 
# mixing python, sql
df = productDf.selectExpr("product_name", "upper(product_name) as title", 
                          "price", "price  * .9 as grand_total")

df.printSchema()
df.show()

# COMMAND ----------


# derived a new column called offer_price, adding new column from existing columns
df = productDf.withColumn("offer_price", productDf.price * 0.9)
df.printSchema()
df.show()

# COMMAND ----------


# referencing columns in data frame

# all below code shall print Column type
print (productDf.price) # specific to productDf
print (productDf["price"]) # specific to productDf

import pyspark.sql.functions as F

print (F.col("price")) # more generic

# when two dataframe has same column name, then use specific one
# column from only 1 dataframe, use specific or generic one
# when data frame is not ready with new column, then use generic one

# COMMAND ----------


# rename column
df = productDf.withColumnRenamed("price", "total")
df.printSchema()
df.show()

# COMMAND ----------


# drop Columns
df = productDf.drop("brand_id")
df.printSchema()
df.show()

# COMMAND ----------


# filter, where conditions
# filter and where are same, alias
# python expression
df = productDf.filter( (productDf.price >= 500) & (productDf["price"] < 600))
df.printSchema()
df.show()

# COMMAND ----------


# filter and where are same
df = productDf.where( (productDf.price >= 500) & (productDf["price"] < 600))
df.printSchema()
df.show()

# COMMAND ----------


productDf.select("product_name").show()

# COMMAND ----------

# pyspark, filter, or where with sql expression, MIX
df = productDf.where (" price >= 500 AND price < 600")
df.printSchema()
df.show()


# COMMAND ----------

# how to reference columns in pyspark
print(productDf.price)
print(productDf['price'])

# with function col - column
from pyspark.sql.functions import col
print(col("price"))

# COMMAND ----------


# add a new column, which a fixed constant
from pyspark.sql.functions import lit 
# lit - literal - constant
# add column qty with constant value 4
# add new column amount by multiplying price * qty
# we cannot use df.qty in below code because qty is not yet ready it is undefined
df = productDf.withColumn("qty", lit(4))\
              .withColumn("amount", col("qty") *  productDf.price)

df.printSchema()
df.show()

# COMMAND ----------


# sort data ascending order
df = productDf.sort("price")
df.show()

# COMMAND ----------


# sorting decending order
from pyspark.sql.functions import desc
df = productDf.sort(desc("price"))
df.show()

# COMMAND ----------


# alternatively use dataframe columns if we have df reference
df = productDf.sort (productDf.price.asc())
df.show()
# desc
df = productDf.sort (productDf.price.desc())
df.show()


# COMMAND ----------

# now fillna /non available
productDf.show()
df = productDf.fillna(value=0) # null value is replaced with 0 value
df.show()

# COMMAND ----------


# now fillna /non available, limit to specific columns
productDf.show()
df = productDf.fillna(value=0, subset=['offer']) # null value is replaced with 0 value
df.show()

# COMMAND ----------

