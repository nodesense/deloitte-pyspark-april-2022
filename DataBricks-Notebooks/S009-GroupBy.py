# Databricks notebook source
# Databricks notebook source
numbersRdd = sc.parallelize(range(1, 10))
# try to split odd/even number as group
# n % 2 = the output shall be either 0 or 1, use this for grouping the numbers
# groupBy used to group by any data in your record
# groupByKey  where only key is used
# lambda either return 0 or 1, the items are grouped based that result
resultRdd = numbersRdd.groupBy(lambda n: n % 2)
# groupByRdds collect won't return list or tuple directly, we need to iterate and 

# n % 2 = return either 1 or 0, then twp groups , group 0, group 1
# get the output
results = resultRdd.collect()
# result is a list of tuple 
# result = [ (0, iterator), (1, iterator) ]
# 0 - even number group
# 1 - odd number group
for key, valueItr in results:
    print("Group ", key, valueItr)
    # iterator the result to get the actual data
    for value in valueItr:
        print ("\tVAlue ", value)

# COMMAND ----------


data = [ 
  ('INFY', 100, 'IT'),
  ('INFY', 200, 'IT'),
  ('LT', 50, 'IT'),
  ('MINDTREE', 150, 'IT'),
  ('INFY', 250, 'IT'),
  ('MARUTI', 7000, 'AUTOMOBILE'),
  ('TATA', 450, 'AUTOMOBILE'),
]

stocksRdd = sc.parallelize(data)

sectorWiseGroup = stocksRdd.groupBy(lambda stock: stock[2]) # stock[2] IT or AUTOMOBILE

results = sectorWiseGroup.collect()

for key, valueItr in results:
    print("Group ", key, valueItr)
    # iterator the result to get the actual data
    for value in valueItr:
        print ("\tVAlue ", value)




# COMMAND ----------


# Keys are symbol, value is a tuple
data = [ 
  ('INFY', (100, 'IT') ),
  ('INFY', (200, 'IT') ),
  ('LT', (50, 'IT')),
  ('MINDTREE', (150, 'IT')),
  ('INFY', (250, 'IT')),
  ('MARUTI', (7000, 'AUTOMOBILE')),
  ('TATA', (450, 'AUTOMOBILE')),
]

stocksRdd = sc.parallelize(data)

results = stocksRdd.groupByKey()
 
groupByResult = results.collect()
  
print(groupByResult)

for key, valueItr in groupByResult:
    print("Group ", key, valueItr)
    # iterator the result to get the actual data
    for value in valueItr:
        print ("\tVAlue ", value)


# COMMAND ----------

