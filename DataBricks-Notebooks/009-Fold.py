# Databricks notebook source
# Databricks notebook source
# fold is useful for aggregate
# fold has initial value to start with where as reduce will  take first value as reduce

def add(acc, value):
    result = acc + value
    print("acc", acc, "value", value, "output", result)
    return result

# COMMAND ----------

rdd = sc.parallelize([10,20,30,40,50])


# COMMAND ----------

# fold with aggregate with start value 0
# fold is action method
# fold works with each partition first, calculate add function on each partition
# + it will apply result of all paritions into again another folder
# return value of add is passed as input with next number in seq

# after processing data from partition 0, it got result 150
# then it will apply add function across partition result  acc 0 value 150
result = rdd.fold (0, add) # output shall be 150 = 10 + 20 + 30 + 40 + 50

print("Fold output is ", result)

# COMMAND ----------

# FoldByKey
# similar to fold, where as fold is applied on all the values in RDD in partition
# foldByKey is used against (Key,Value) paired rdd, key/value rdd
# fold work based on key

orders = [
    # symbol, qty
    ('INFY', 200),
    ('TSLA', 50),
    ('EMR', 20),
    ('INFY', 100),
    ('TSLA', 25)
]

def add(acc, value):
    output = acc + value
    print("acc", acc, "value", value, "output", output)
    return output

orderRdd = sc.parallelize(orders)
# fold by Key, return rdd
# When key appear first, it starts with 0, and value
# second appearance key, include previous output as input
result = orderRdd.foldByKey(0, add).collect()

print("Result ", result)
# COMMAND ----------


# COMMAND ----------

