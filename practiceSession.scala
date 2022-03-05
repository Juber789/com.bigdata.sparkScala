package VenuClasses.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object practiceSession {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession.builder.master("local[*]").appName("practiceSession").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val data1 = "C:\\bigdata\\datasets\\regions.csv"
    val df1 = spark.read.option("header","true").option("inferSchema","true").option("quote","\'").csv(data1)
    df1.printSchema()
    df1.show(5,false)

    val cols1 = df1.columns.map(x=>x.toLowerCase)
    val ndf1 = df1.toDF(cols1:_*)
    ndf1.show(false)

    spark.stop()
  }
}