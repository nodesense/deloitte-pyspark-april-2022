# Databricks notebook source
# Databricks notebook source
products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), #   no matching brand
         (4, 'Pixel', 400),
]

brands = [
    #(brand_id, brand_name)
    (100, "Apple"),
    (200, "Samsung"),
    (400, "Google"),
    (500, "Sony"), # no matching products
]



productDf = spark.createDataFrame(data=products, schema=["product_id", "product_name", "brand_id"])
productDf.show()

brandDf = spark.createDataFrame(data=brands, schema=["brand_id", "brand_name"])

brandDf.show()

store = [
    #(store_id, store_name)
    (1000, "Poorvika"),
    (2000, "Sangeetha"),
    (4000, "Amazon"),
    (5000, "FlipKart"), 
]
 
storeDf = spark.createDataFrame(data=store, schema=["store_id", "store_name"])
storeDf.show()

# COMMAND ----------

# Inner Join
# productDf is left
# brandDf is right
# select/pick only matching record, discord if no matches found
df = productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "inner")
df.printSchema()
df.show()


# COMMAND ----------


# Outer Join, Full Outer Outer, [Left outer + Right outer]
# pick all records from left dataframe, and also right dataframe
# if no matches found, it fills null data for not matched records
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "outer").show()

# COMMAND ----------


# Left, Left Outer join 
# picks all records from left, if no matches found, it fills null for right data
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "leftouter").show()


# COMMAND ----------


# Right, Right outer Join
# picks all the records from right, if no matches found, fills left data with null
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "rightouter").show()

# COMMAND ----------


# left semi join
# join in general convention, it pull the records from left, join them based on condition with right table
# left semi join, join left and right based on condition, however it pull the records only from left side

# it is similar to innerjoin, but pick/project records only from left
# we only see left datafram columns, not right side columns
# we can't see brand_id, brand_name from brands df
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "leftsemi").show()


# COMMAND ----------


# left anti join: exact opposite to semi join
# picks the records that doesn't have match on the right side
productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "leftanti").show()

# COMMAND ----------


# cartesian , take row from left side, match with all from right side
productDf.crossJoin(storeDf).show()

# COMMAND ----------

