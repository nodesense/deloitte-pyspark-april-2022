# Databricks notebook source
# Databricks notebook source
# foreach 
# apply a function to each element in the rdd [all partitions]
# write any custom functions that deal with db, datastore, cache etc


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

orderRdd = sc.parallelize(orders, 2)
# refer S015-Fold for example,
resultRdd = orderRdd.foldByKey(0, add)
resultRdd.collect()


# COMMAND ----------


# resultRdd has result, now we apply foreach, it may store data to data store
# good for processing ONE RESULT at a time
# foreach executed in executor process, not in driver
def updateDB(stock):
    #Todo, update, insert, delete record
    print("Saving ", str(stock), " to db ")
    
# foreach is ACTION method
resultRdd.foreach(updateDB)


# COMMAND ----------


# foreachPartition
# custom logic to handle data in the partitions
# runs inside executors
# foreach process 1 element at a time, 
# where as foreachPartition can process all 
# partition data as bulk
# bulk insert/update/delete

# iterator will have each partition data as whole
# part0 - 5 records, then iterator shall have 5 records
# processData is called by foreachPartition on executor for each partition
# iterator passed for foreachPartition
def processData(iterator):
    print("Process data called ")
    for record in iterator:
        print ("Processing ", str(record))
        
    print ('-' * 30)
# Action method
resultRdd.foreachPartition(processData)
print("Show on notebook")

# COMMAND ----------

