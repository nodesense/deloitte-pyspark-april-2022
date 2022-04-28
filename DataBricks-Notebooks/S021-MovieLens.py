# Databricks notebook source
# /FileStore/tables/ml-latest-small/movies.csv
# /FileStore/tables/ml-latest-small/ratings.csv

# how to create schema programatically instead of using inferSchema
from pyspark.sql.types import StructType, LongType, StringType, IntegerType, DoubleType
# True is nullable, False is non nullable
movieSchema = StructType()\
                .add("movieId", IntegerType(), True)\
                .add("title", StringType(), True)\
                .add("genres", StringType(), True)

ratingSchema = StructType()\
                .add("userId", IntegerType(), True)\
                .add("movieId", IntegerType(), True)\
                .add("rating", DoubleType(), True)\
                .add("timestamp", LongType(), True)
  

# COMMAND ----------

# read movie data
# read using dataframe with defind schema
# we can use folder path - all csv in the folder read
# use file path, only that file read

# spark is session, entry point for data frame/sql

movieDf = spark.read.format("csv")\
                .option("header", True)\
                .schema(movieSchema)\
                .load("/FileStore/tables/ml-latest-small/movies.csv")

movieDf.printSchema()
movieDf.show(2) # truncate column width by default
movieDf.show(2, truncate=False) # shall not truncate, wider column in output

# COMMAND ----------

ratingDf = spark.read.format("csv")\
                .option("header", True)\
                .schema(ratingSchema)\
                .load("/FileStore/tables/ml-latest-small/ratings.csv")

ratingDf.printSchema()
ratingDf.show(2)

# COMMAND ----------

ratingDf.select("rating").distinct().sort(ratingDf.rating.asc()).show()

# COMMAND ----------

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find the movies by total ratings by userId
df = ratingDf\
     .groupBy("movieId")\
     .agg(count("userId").alias("total_ratings"))\
     .sort(desc("total_ratings"))

df.printSchema()
df.show(20)

# COMMAND ----------


# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count, min, max

# find  average rating by users sorted by desc
df = ratingDf\
     .groupBy("movieId")\
     .agg(avg("rating").alias("avg_rating"), min("rating"), max("rating"), count("userId"))\
     .sort(desc("avg_rating"))

df.printSchema()
df.show(200)

# COMMAND ----------

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find  the most popular movies, where as rated by many users, at least movies should be rated by 100 users
# and the average rating should be at least 3.5 and above
# and sort the movies by total_ratings
mostPopularMoviesDf = ratingDf\
     .groupBy("movieId")\
     .agg(avg("rating").alias("avg_rating"), count("userId").alias("total_ratings") )\
     .filter( (col("total_ratings") >= 100) & (col("avg_rating") >=3.5) )\
     .sort(desc("total_ratings"))
     

mostPopularMoviesDf.cache() # MEMORY

mostPopularMoviesDf.printSchema()
mostPopularMoviesDf.show(20)


# COMMAND ----------


# join, inner join 
# get the movie title for the mostPopularMoviesDf
# join mostPopularMoviesDf with movieDf based on condition that mostPopularMoviesDf.movieId == movieDf.movieId
from pyspark.sql.functions import round

popularMoviesDf = mostPopularMoviesDf.join(movieDf, mostPopularMoviesDf.movieId == movieDf.movieId)\
                                     .select(movieDf.movieId, "title", "avg_rating", "total_ratings")\
                                     .withColumn("round", round("avg_rating", 2))\
                                     .sort(desc("total_ratings"))

popularMoviesDf.cache()

popularMoviesDf.show(100)

# COMMAND ----------


# display physical plan
mostPopularMoviesDf.explain()

# COMMAND ----------


mostPopularMoviesDf.explain(extended = True)

# COMMAND ----------


popularMoviesDf.explain(extended = True)


# COMMAND ----------


# write popularMoviesDf to hadoop with header [by default headers shall not be written]
# overwrite existing files
# 70 plus partitions having approx total of 100 plus records
# write 70 plus files into hadoop
print(popularMoviesDf.rdd.getNumPartitions())
# write statements work on exeuctors where each partitions information stored
# executor the one write the results to file system
popularMoviesDf.write.mode("overwrite")\
                .option("header", True)\
                .csv("/FileStore/tables/tmp/popular-movies-csv-files")


# COMMAND ----------


# write popularMoviesDf into single file
# coalesce(1) to reduce partitions
popularMoviesDf.coalesce(1).write.mode("overwrite")\
                .option("header", True)\
                .csv("/FileStore/tables/tmp/popular-movies-csv")


# COMMAND ----------


# inferSchema will scan csvs and define data types for  youy schema
popularMovies = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema", True)\
                .load("/FileStore/tables/tmp/popular-movies-csv-files")

popularMovies.printSchema()
print("Partitions", popularMovies.rdd.getNumPartitions())
popularMovies.show()

# COMMAND ----------


# inferSchema will scan csvs and define data types for  youy schema
popularMovies = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema", True)\
                .load("/FileStore/tables/tmp/popular-movies-csv")

popularMovies.printSchema()
print("Partitions", popularMovies.rdd.getNumPartitions())
popularMovies.show()

# COMMAND ----------


# write popularMoviesDf into single file
# coalesce(1) to reduce partitions
popularMoviesDf.coalesce(1).write.mode("overwrite")\
                .parquet("/FileStore/tables/tmp/popular-movies-parquet")


# COMMAND ----------


# write popularMoviesDf into single file
# coalesce(1) to reduce partitions
popularMoviesDf.coalesce(1).write.mode("overwrite")\
                .json("/FileStore/tables/tmp/popular-movies-json")

# COMMAND ----------


# write popularMoviesDf into single file
# coalesce(1) to reduce partitions
popularMoviesDf.coalesce(1).write.mode("overwrite")\
                .orc("/FileStore/tables/tmp/popular-movies-orc")

# COMMAND ----------


popularMovies = spark.read.format("json")\
                .option("inferSchema", True)\
                .load("/FileStore/tables/tmp/popular-movies-json")

popularMovies.printSchema()
print("Partitions", popularMovies.rdd.getNumPartitions())
popularMovies.show()


# COMMAND ----------


# parquet has native column, schema
popularMovies = spark.read.format("parquet")\
                .load("/FileStore/tables/tmp/popular-movies-parquet")

popularMovies.printSchema()
print("Partitions", popularMovies.rdd.getNumPartitions())
popularMovies.show()

# COMMAND ----------

# orc has native column, schema
popularMovies = spark.read.format("orc")\
                .load("/FileStore/tables/tmp/popular-movies-orc")

popularMovies.printSchema()
print("Partitions", popularMovies.rdd.getNumPartitions())
popularMovies.show()

# COMMAND ----------

