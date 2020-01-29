package org.example

import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, expressions, _}
import org.apache.spark.sql.functions.{col,substring,dayofmonth, dayofweek, hour, monotonically_increasing_id,
  month,
  posexplode, to_date, to_timestamp, year}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
case class TestPerson(name: String, age: String, salary: String,des:Int,base: String)
object App3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;
    var df0 = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/jaynab_khatun/com.spark/src/main/resources/Data/basepayment.csv")
    df0.show()
    import spark.implicits._
    val df1 = Seq((1,"1,2,3","action|thriller|romance"),(2,"1,2,3","fantastic|action")).toDF("Id","name","genre")
    df1.show()

    val df = Seq(
      (Seq("2019", "2018", "2017"), Seq("100", "200", "300"), Seq("IN", "PRE", "POST")),
      (Seq("2018"), Seq("73"), Seq("IN")),
      (Seq("2018", "2017"), Seq("56", "89"), Seq("IN", "PRE")))
      .toDF("Date","Amount", "Status")
      .withColumn("idx", monotonically_increasing_id)

    df.columns.filter(_ != "idx").map{
      c => df.select($"idx", posexplode(col(c))).withColumnRenamed("col", c)
    }
      .reduce((ds1, ds2) => ds1.join(ds2, Seq("idx", "pos")))
      .select($"Date", $"Amount", $"Status", $"pos".plus(1).as("Sequence"))
      .show




    import spark.implicits._
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.Dataset
    val df3 = spark.sparkContext.parallelize(Seq((Seq("year","month","day","hour"), Seq(1,2,3,4), Seq("MM/dd/yyyy",
      "MM/dd/yyyy","MM/dd/yyyy","MM/dd/yyyy"), "TimeStamp","IPMTDT"),(Seq("Country", "ZipCode"), Seq(5,6), Seq
    ("substring(IPMTDT,1,2)","substring(IPMTDT,3,5)"), "String","IPMTDT"), (Seq("deal_id"),Seq(7), Seq(""), "String",
      "deal_id")))
      .toDF()
    df3.show(false)
    val transformedDF1 = df3
      .flatMap{case Row(part_col: Seq[String], order: Seq[Int], format: Seq[String],datatype:String,base:String) =>
        part_col.indices.map(index => (part_col(index), order(index), format(index),datatype,base))}
      .toDF("Partition_column", "partition_orer", "rdbms_format","datatype","base")
    transformedDF1.show(false)

     /*var PersonList:ListBuffer[TestPerson] = ListBuffer[TestPerson]()
      transformedDF1.foreach { y =>
        var t :TestPerson = TestPerson(y._5, y._4, y._3, y._2, y._1)
        //println(t)
        PersonList += t
        println(PersonList.toList)
      }
    print(PersonList)*/
    //df0 = df0.withColumn("TimeStamp", to_timestamp(col("IPMTDT"),fmt = "MM/dd/yyyy"))
    //df0.select("TimeStamp").show()
    //df0 = df0.withColumn("To_date", to_date(col("IPMTDT"),fmt = "MM/dd/yyyy"))
    //df0.select("To_date").show()
   // df0 = df0.withColumn("Country", substring(col("BATCH_ID"),1,2))
  //df0.show()

    /*val rdbms_format ="substring(IPMTDT,3,5)"
    val pattern= "[0-9]".r
    val match1=pattern.findAllIn(rdbms_format).toSeq
    println("pos"+ match1(0)+"length"+match1(1))*/
    transformedDF1.collect().foreach { y =>
      val tp = y.toSeq
      val Partition_column = tp(0).toString
      val rdbms_format = tp(2).toString
      val datatype = tp(3).toString
      val base = tp(4).toString
     // println(Partition_column+" "+rdbms_format+" "+ datatype+" "+base)

    /*if (datatype == "TimeStamp" && Partition_column != base) {
        val newcol = Partition_column.toString
        println(newcol)
      if(newcol=="year"){
        df0 = df0.withColumn(newcol, year(to_timestamp(col(base),fmt = rdbms_format)))
      }
      if(newcol=="month"){
        df0 = df0.withColumn(newcol, month(to_timestamp(col(base),fmt = rdbms_format)))
      }
      if(newcol=="day"){
        df0 = df0.withColumn(newcol, dayofmonth(to_timestamp(col(base),fmt = rdbms_format)))
      }
      if(newcol=="hour"){
        df0 = df0.withColumn(newcol, hour(to_timestamp(col(base),fmt = rdbms_format)))
      }

      /*newcol match {
          case "year" => df0 = df0.withColumn(newcol, year(to_timestamp(col(base),fmt = rdbms_format)))
          case "month" => df0 = df0.withColumn(newcol, month(to_timestamp(col(base),fmt = rdbms_format)))
          case "date" => df0 = df0.withColumn(newcol, dayofmonth(to_timestamp(col(base),fmt = rdbms_format)))
          case "hour" => df0 = df0.withColumn(newcol, year(to_timestamp(col(base),fmt = rdbms_format)))
          println("inside case")
        }*/
      }*/
    if (rdbms_format.startsWith("substr") && datatype != null && Partition_column != base) {
        val newcol = Partition_column.toString
        val pattern= "[0-9]".r
        val match1=pattern.findAllIn(rdbms_format).toSeq
       val pos= match1(0).toInt
       val lenght=match1(1).toInt
       print(pos+" "+lenght)
        df0 = df0.withColumn(newcol, substring(col(base),pos,lenght))

      }
    }
    //df0.select("IPMTDT","year","month","day","hour").show(20)
   df0.show()





    //var sortedDF = transformedDF1.orderBy("partition_orer")
    //var newpartcols=ListBuffer("BATCH_ID")
   // val sdf=sortedDF.select("Partition_column").map(r => r.getString(0)).collect.toList
    //newpartcols ++= sdf
    //println(newpartcols)
    //var df01=df0
   //sortedDF.collect.foreach { y=>

      //if (y.base != y.partition_column && y.rdbms_format == "TimeStamp") {
         //if(newcol == "year") df01 = df01.withColumn(newcol,year(to_date(df((base )),baseformat)))
       // if(newcol =="month") df01 = df01.withColumn(newcol,month(to_date(df((base )),baseformat)))


    //}
 //df01.show(false)
  }


}



