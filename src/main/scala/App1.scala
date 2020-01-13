import java.io.File

import org.apache.commons.math3.geometry.spherical.oned.ArcsSet.Split
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.TimestampType

object App1 {
  def main(args: Array[String]): Unit = {
    println( "dshjs")
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;
    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/jaynab_khatun/com.spark/src/main/resources/Data/data.csv")
    print("sdf")
    df.show()
   // val partitionDF= df.select(year(col("UPD_DATE")).alias("Year"))//,month(col("UPD_DATE")).alias("Month"))
   // val yearDF=df.withColumn("Year",year(df("UPD_DATE")))
    //yearDF.show()
   //val resDF = df.withColumn( "timestamp",unix_timestamp(df("UPD_DATE"), "MM/dd/yy mm:ss").cast(TimestampType))
   //resDF.printSchema()
    //resDF.show()
    //val yearDF=resDF.withColumn("Year",year(resDF("timestamp")))
    val yearDF1=df.withColumn("Year",year(unix_timestamp(df("UPD_DATE"),"MM/dd/yy mm:ss").cast(TimestampType)))
    println("YearDF is showing")
    yearDF1.show()
    /*val partitionDF= yearDF.select(year(col("timestamp")).alias("Year"),month(col("timestamp")).alias("Month"))
    partitionDF.show()

    val tempDF= resDF.withColumn("Year", year(resDF("timestamp"))).withColumn("Month",month(resDF("timestamp")))
    tempDF.show()
    println("end")*/
  }
}
