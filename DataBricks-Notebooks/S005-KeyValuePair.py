# Databricks notebook source
# Databricks notebook source
# key, value pair RDD 
# tuple , immutable data strcuture similar to list
# tuple can be created using ( )
# tuple ('apple', 2) , apple is key, 2 is value for key value rdd

# Key Value pair formed from tuple, where as first element in tuple is known as key
# second element known as value
# (key, value)
# apple is key, 20 is value
data = [
    ('apple', 20),
    ('orange', 30),
    ('apple', 10),
    ('mango', 50),
    ('apple', 5)
]

rdd = sc.parallelize(data)

# COMMAND ----------


# find the count keys
# for rdd functions with "Key" can use this dataset

result = rdd.countByKey() # action
result # result is dictionary, it returns count of keys


# COMMAND ----------


# find the total kilogram of fruits sold (apple - 30, orange - 30, mongo - 50)
# reduceByKey , useful for sum or custom code
# lambda function here woun't be called first time when key found
"""
Input
('apple', 20) <- apple is first time, this value directly placed in table, lambda not called
('orange', 30) <- orange is first time, this value directly placed in table, lambda not called
('apple', 10) <- apple is second time, lambda shall be called
                 lambda has two params, acc, value
                 acc value taken from table shown below, value from record 
                 acc = 20, value  10
                 (20, 10) => acc + value = (20 + 10) = 30, this value updated in table
('mango', 50)<- first time, lambda not called, values directly placed
('apple', 5) <- apple 3rd time, lambda shall be called acc = 30 from table, value 5 from input
                 (30, 5) => 30 + 5 = 35, shall be updated in table
Virtaually there is table
Key       acc
apple     35
orange    30
mango     50

"""
# acc is just variable, called accumulator
# value is from rdd, 20, 30, 10 ,50
result = rdd.reduceByKey(lambda acc, value: acc + value)
result.collect()

# COMMAND ----------

