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

# in any spark application, there will be ONLY ONE spark context
# and as many spark sessions allowed

spark2 = spark.newSession()

# COMMAND ----------

spark2

# COMMAND ----------

# create product temp table in spark session
productDf.createOrReplaceTempView("products")

# COMMAND ----------

# now access products from session /knew it will work
spark.sql("SELECT * FROM products").show()

# COMMAND ----------

# now try to access products from spark2, IT WILL FAIL, as products table private to spark session
spark2.sql("SELECT * FROM products").show() # error  AnalysisException: Table or view not found: products; 

# COMMAND ----------

# now create global temp view global_temp that can be shared across multiple sessions on same notebook

brandDf.createOrReplaceGlobalTempView ("brands")

spark.sql("SHOW TABLES IN global_temp").show()

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.brands").show()


# COMMAND ----------

# access global temp from spark2 session
spark2.sql("SELECT * FROM global_temp.brands").show()


# COMMAND ----------

