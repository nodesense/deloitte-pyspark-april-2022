// Databricks notebook source
// Databricks notebook source
  // Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
// check derby db exist or not, jar, classes
// derby used with hive etc
Class.forName("org.apache.derby.jdbc.EmbeddedDriver");

// COMMAND ----------


//jdbc:derby:memory:myDB;create=true
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


// COMMAND ----------


val conn = DriverManager.getConnection("jdbc:derby:memory:myDB;create=true")


// COMMAND ----------


val query = "create table test(id int)";
val stmt = conn.createStatement()
stmt.executeUpdate(query);

// COMMAND ----------


val query = "insert into  test(id) values(1)";
val stmt = conn.createStatement()
stmt.executeUpdate(query);

// COMMAND ----------


val query = "select * from test";
val stmt = conn.createStatement()
val rs = stmt.executeQuery(query);

while (rs.next()) {
        val  id = rs.getString("id");
        println(id)
}

// COMMAND ----------


// read using Scala, Python DataFrame
val dataframe = spark.read.format("jdbc")
                      .option("url", "jdbc:derby:memory:myDB;create=true")
                      .option("driver", "org.apache.derby.jdbc.EmbeddedDriver")
                      .option("dbtable", "test")
                      //.option("user", "root")
                      //.option("password", "root")
                      .load()


// COMMAND ----------

dataframe.printSchema()


// COMMAND ----------

dataframe.show()

// COMMAND ----------


// create temp view out of data frame
dataframe.createOrReplaceTempView("testdf")

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC SELECT * from testdf

// COMMAND ----------


Seq(2,3,4,5)
.toDS()
.withColumnRenamed("value", "id")
.write
 .mode("append")
.format("jdbc")
.option("url", "jdbc:derby:memory:myDB;create=true")
.option("driver", "org.apache.derby.jdbc.EmbeddedDriver")
.option("dbtable", "test")
 .save()



// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * from testdf

// COMMAND ----------


"""
|-- movieId: integer (nullable = true)
 |-- title: string (nullable = true)
 |-- avg_rating: double (nullable = true)
 |-- total_ratings: long (nullable = false)
 |-- genres: string (nullable = true)
"""

val query = "create table popular_movies( movieId int,   title varchar(500), avg_rating FLOAT,   total_ratings bigint,    genres varchar(500)   )"
val stmt = conn.createStatement()
stmt.executeUpdate(query);


// COMMAND ----------

