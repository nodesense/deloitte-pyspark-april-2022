# Databricks notebook source
# Databricks notebook source
sector_dict = {
        "MSFT": "TECH",
        "TSLA": "AUTO",
        "EMR": "INDUSTRIAL"
}

stocks = [
    ("EMR", 52.0),
    ("TSLA", 300.0),
    ("MSFT", 100.0)
]


# create broadcasted variabel using Spark Context
# this will ensure that broadCastSectorDict  is kept in every executor 
# where task shall be running
# lazy evaluation, data shall be copied to executors when we run the job
broadCastSectorDict = sc.broadcast(sector_dict)

# COMMAND ----------

stocksRdd = sc.parallelize(stocks)


# COMMAND ----------

# Pyspark see this code, this has reference to broadCastSectorDict
# which is broardcast data, pyspark place the broadCastSectorDict in every executor
# 1 time instead of coping with  every task

# without broadCast, sector_dict shall be copied to executor for every task
# add sector code with stock at executor level when task running
def enrichStockWithSector(stock):
    return stock + (broadCastSectorDict.value[stock[0]] ,)

# code marshalling - Python copy the lamnda code to executor system/processor
# now enrichStockWithSector also shipped to executor on every task
enrichedRdd = stocksRdd.map (lambda stock: enrichStockWithSector(stock))

enrichedRdd.collect()

# COMMAND ----------

