package VenuClasses.spark.FirstClass.CSV

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object US_data {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession.builder.master("local[*]").appName("sparkFirstClass").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql


    // Creating DataFrame using csv file
    val data = "C:\\bigdata\\datasets\\us-500.csv"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(data)
    df.printSchema()
    df.show(5, false)

    //Run SQL Query on top of Data Frame
    /*df.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab")
    res.show(5,false)
    val res1 = spark.sql("select * from tab where state = 'NJ' and email like '%gmail.com'")
    res1.show(false)
    val res2 = spark.sql("select state, count(*) as cnt from tab group by state order by cnt desc")
    res2.show(false)*/

    //DSL(Domian Specific Language) Using spark-scala
    val res1 = df.where($"state"==="NJ" && $"email".like("%gmail.com"))
    res1.show(5,false)
    val res2 = df.groupBy($"state").agg(count("*").alias("cnt")).orderBy($"cnt".desc)
    res2.show(6,false)


    spark.stop()
  }
}