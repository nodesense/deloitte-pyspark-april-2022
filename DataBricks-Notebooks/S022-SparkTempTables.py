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

# sql returns data frame
# look up is on Hive Meta store [meta store wil have databsaes, tables, columns etc]
df = spark.sql("SHOW DATABASES")
df.show()

# COMMAND ----------

# Nature of tables
#  Temp tables/View [default] - stored in memory as partitions
#  Global Temp Tables/View [default] - stored in memory as partitions
#  Permanent Tables 
#  Permanent Tables has two types
#          1. Managed Table
#          2. External Table

# COMMAND ----------

spark.sql("SHOW TABLES IN default").show()

# COMMAND ----------

# temp table/view, where data is in memory only not persisted
# one way to create temp table is to create from dataframe
# intellisense - <TAB><TAB><TAB>
# this create a temp table /view called products in default db
# products temp table avilable only on particular spark session
productDf.createOrReplaceTempView("products")
brandDf.createOrReplaceTempView("brands")

# COMMAND ----------

spark.sql("SHOW TABLES IN default").show() # must display two temp tables

# COMMAND ----------

df = spark.sql("SELECT * FROM products")
df.printSchema()
df.show()

# COMMAND ----------

df = spark.sql("SELECT * FROM brands")
df.printSchema()
df.show()

# COMMAND ----------

# databricks has display function to display dataframe in html5
# show function is ASCII format
# display is HTML format

df = spark.sql("SELECT * FROM products")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- sql comment
# MAGIC -- we can write query directly here
# MAGIC -- magic function, the query here taken and run on spark.sql automatically by databricks notebook and result Dataframe is displayed using display function
# MAGIC 
# MAGIC SELECT * FROM products

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM brands

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT brand_id, brand_name, upper(brand_name), lower(brand_name) from brands;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- inner join
# MAGIC SELECT * from products 
# MAGIC INNER JOIN brands ON products.brand_id = brands.brand_id 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- full outer join
# MAGIC SELECT * from products 
# MAGIC FULL OUTER JOIN brands ON products.brand_id = brands.brand_id 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- left outer join
# MAGIC SELECT * from products 
# MAGIC LEFT OUTER JOIN brands ON products.brand_id = brands.brand_id 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- right outer join
# MAGIC SELECT * from products 
# MAGIC RIGHT OUTER JOIN brands ON products.brand_id = brands.brand_id 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- left semi   join
# MAGIC SELECT * from products 
# MAGIC LEFT SEMI  JOIN brands ON products.brand_id = brands.brand_id 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- left anti   join
# MAGIC SELECT * from products 
# MAGIC LEFT ANTI  JOIN brands ON products.brand_id = brands.brand_id 

# COMMAND ----------

storeDf.createOrReplaceTempView("stores")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- cross join
# MAGIC 
# MAGIC SELECT * from products 
# MAGIC CROSS JOIN stores 

# COMMAND ----------

