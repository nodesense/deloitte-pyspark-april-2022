-- Databricks notebook source
-- MAGIC %python
-- MAGIC # spark database
-- MAGIC # involves two areas
-- MAGIC #      Meta Data [databases, tables in side databases, columns, data types, location where data located, table type]
-- MAGIC #      Data itself as CSV, json, orc, parquet format, stored in DBFS, S3, HDFS etc
-- MAGIC 
-- MAGIC # Spark SQL Database
-- MAGIC # Meta data - Hive Meta server
-- MAGIC # Query Engine - Spark
-- MAGIC 
-- MAGIC # two types table
-- MAGIC 
-- MAGIC # Managed Table: Data Manipulation like insert/update/delete to be performed using spark sql
-- MAGIC #                Default location for managed table shall be /usr/hive/warehouse location
-- MAGIC #                Meta data [table, columns] is stored in Hive Meta
-- MAGIC 
-- MAGIC # External Table: Data Manipulation like inset/update/delete is not managed by SPARK
-- MAGIC #                  Data Manipulation are handled by ETL externally, add file/remove  file/update file
-- MAGIC #                 Data is stored anyware in data lake like S3, HDFS etc
-- MAGIC #                Meta data [table, columns] is stored in Hive Meta

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # delete exisitng data 
-- MAGIC # we have existing data from prev workshop
-- MAGIC dbutils.fs.rm("/user/hive/warehouse/moviedb.db", True)

-- COMMAND ----------

DROP DATABASE if exists moviedb

-- COMMAND ----------

-- database is created in hive meta
-- managed table data  stored in /usr/hive/warehouse/moviedb.db
CREATE DATABASE if not exists moviedb

-- COMMAND ----------

-- delta tables [new feature]
-- managed table, mean data should be inserted/updated/deleted by spark only
CREATE TABLE if not exists moviedb.reviews (id INT, review STRING)
-- data shall be stored in /user/hive/warehouse/moviedb.db/reviews

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

SHOW TABLES in moviedb

-- COMMAND ----------

-- describe table
DESC FORMATTED moviedb.reviews

-- COMMAND ----------

-- delta table, allows insert/update/delete
INSERT INTO moviedb.reviews VALUES(1, 'nice movie')

-- COMMAND ----------

SELECT * FROM moviedb.reviews

-- COMMAND ----------

UPDATE moviedb.reviews SET review='comedy movie' where id=1

-- COMMAND ----------

SELECT * FROM moviedb.reviews

-- COMMAND ----------

DELETE FROM moviedb.reviews WHERE id=1;

-- COMMAND ----------

SELECT * FROM moviedb.reviews

-- COMMAND ----------

SHOW TABLES in moviedb

-- COMMAND ----------

-- Type EXTERNAL
DESC FORMATTED moviedb.movies_csv

-- COMMAND ----------

-- print create table syntax for exisitng table
SHOW CREATE TABLE moviedb.movies_csv

-- COMMAND ----------

-- DROP  TABLE if   EXISTS moviedb.ratings_csv

-- COMMAND ----------

-- userId,movieId,rating,timestamp
CREATE TABLE if not EXISTS moviedb.ratings_csv ( 
                  userId INT, 
                  movieId INT, 
                   rating DOUBLE  ,
                   ts LONG
                  ) 
 USING csv 
 OPTIONS ( 'delimiter' = ',', 'escape' = '"', 'header' = 'true', 'multiLine' = 'false') 
 LOCATION 'dbfs:/FileStore/tables/ml-latest-small/ratings.csv'

-- COMMAND ----------

DESC FORMATTED  moviedb.ratings_csv

-- COMMAND ----------

SELECT * FROM moviedb.ratings_csv LIMIT 10

-- COMMAND ----------

SELECT movieid, avg(rating) as avg_rating, count(userId) as total_ratings FROM moviedb.ratings_csv 
GROUP BY movieid
HAVING avg_rating > 3.5 AND total_ratings > 100

-- COMMAND ----------

-- create temp table from query CREATE TABLE AS SELECT
CREATE OR REPLACE TEMP VIEW popular_ratings AS 
SELECT movieid, avg(rating) as avg_rating, count(userId) as total_ratings FROM moviedb.ratings_csv 
GROUP BY movieid
HAVING avg_rating > 3.5 AND total_ratings > 100

-- COMMAND ----------

SHOW TABLES IN default

-- COMMAND ----------

SELECT * FROM popular_ratings

-- COMMAND ----------

-- create a popular_movies table by joning movies_csv and popular_ratings temp view
CREATE OR REPLACE TABLE moviedb.popular_movies   AS 
SELECT pr.movieid, title, avg_rating, total_ratings FROM popular_ratings pr
INNER JOIN moviedb.movies_csv m  ON  pr.movieid = m.movieid

-- COMMAND ----------

SHOW TABLES in moviedb

-- COMMAND ----------

SELECT * FROM moviedb.popular_movies;

-- COMMAND ----------

