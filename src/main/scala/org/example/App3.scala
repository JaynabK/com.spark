package org.example

import org.apache.spark
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object App3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;
    var partcol = Seq("BATCH_ID")
    partcol = partcol :+ "year"
    println(partcol)
  }
}
