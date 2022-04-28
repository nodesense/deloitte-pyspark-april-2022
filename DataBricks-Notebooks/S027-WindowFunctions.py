# Databricks notebook source
# Databricks notebook source
data = [ ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
   ]

empDf = spark.createDataFrame(data=data, schema=['name', 'dept', 'salary'])
empDf.printSchema()
empDf.show()

empDf.rdd.getNumPartitions()

# COMMAND ----------


empDf.rdd.glom().collect()

# COMMAND ----------

empDf.write.mode("overwrite").option("header", True)\
  .partitionBy("dept")\
  .csv("/FileStore/tables/tmp/employees")

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import pyspark.sql.functions as F
# specification for window, partitions, functions that should be applied on partition
# with in department, order the data based on salary in ascending order
windowSpec = Window.partitionBy("dept").orderBy("salary")
# we have apply the spec on dataframe
df = empDf.withColumn("row_number", row_number().over(windowSpec))

df.printSchema()
df.show()

# pick row_number 1 on each department
df.filter(F.col("row_number") == 1).show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# rank with gap with ascending order
"""
score  rank
90      1
90      1
89      3  [gap, 2 not included]
"""
windowSpec = Window.partitionBy("dept").orderBy("salary")

df = empDf.withColumn("rank", rank().over(windowSpec))
df.show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# rank with gap
"""
score  rank
90      1
90      1
89      3  [gap, 2 not included]
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("rank", rank().over(windowSpec))
df.show()

# COMMAND ----------


from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc

# dense_rank ranking without gap
"""
score  rank
90      1
90      1
89      2  
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("dense_rank", dense_rank().over(windowSpec))
df.show()

# COMMAND ----------


from pyspark.sql.window import Window
from pyspark.sql.functions import ntile, desc

# ntile ranking with related certain range for range
# rank shall fit into a range
"""
 
"""
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("ntile", ntile(3).over(windowSpec))
df.show()

# COMMAND ----------

# Analytic functions
# Cumulative distribution - similar to rank, calcualted and values are bound between 
# 0 and 1

# 10 USD per share => 13 USD per share      = 3 USD per share, 30 % gain .3
# 100 USD per share => 110 USD per share    = 10 USD per share, 10% gain .1
# cumulative distribution
from pyspark.sql.window import Window
from pyspark.sql.functions import cume_dist, desc

# similar to  rank  
 
windowSpec = Window.partitionBy("dept").orderBy(desc("salary"))

df = empDf.withColumn("cume_dist", cume_dist().over(windowSpec))
df.show()

# COMMAND ----------


# lag - previous lag
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, desc

windowSpec = Window.partitionBy("dept").orderBy("salary")

df = empDf.withColumn("lag", lag("salary", 1).over(windowSpec))
df.show()


# COMMAND ----------


# lead -  the one ahead, 
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, desc

windowSpec = Window.partitionBy("dept").orderBy("salary")

df = empDf.withColumn("lead", lead("salary",1).over(windowSpec))
df.show()

# COMMAND ----------


# aggregate functions, min, max, sum, count, avg
 
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, sum, min, max, count, col

windowSpec = Window.partitionBy("dept")

df = empDf\
          .withColumn("min", min(col("salary")).over(windowSpec))\
          .withColumn("max", max(col("salary")).over(windowSpec))\
          .withColumn("avg", avg(col("salary")).over(windowSpec))\
          .withColumn("count", count(col("salary")).over(windowSpec))\
          .withColumn("sum", sum(col("salary")).over(windowSpec))

df.show()


# COMMAND ----------

