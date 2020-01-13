package org.example
import org.apache.spark.sql._
import org.apache.spark.sql.types._
//import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{month, to_date, when, year}
import org.apache.spark.sql.types.{DoubleType, IntegerType, MapType, StringType, StructField, StructType, TimestampType}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * Hello world!
 *
 */
case class TestPerson(name: String, age: String, salary: String,des:String)
object App  {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate;
    val df = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED")
      .load("/Users/jaynab_khatun/com.spark/src/main/resources/Data/basepayment.csv")
    df.show()
    val nestedList = ListBuffer(
      List(2,("String","String","String","String","String","String"),1,("String","String","String","String","String","String"),1,("String","String","String","String","String","String")),
      List(3,("String","String","String","String","String","String"),1,("String","String","String","String","String","String"),1,("String","String","String","String","String","String")),
      List(3,("String","String","String","String","String","String"),2,("String","String","String","String","String","String"),1,("String","String","String","String","String","String")),
      List(3,("String","String","String","String","String","String"),2,("String","String","String","String","String","String"),2,("String","String","String","String","String","String")),
      List(3,("String","String","String","String","String","String"),1,("String","String","String","String","String","String"),2,("String","String","String","String","String","String"))
    )
    var t21=nestedList.map(x => (x(0).asInstanceOf[Int], x(1).toString, x(2).asInstanceOf[Int], x(3).toString, x(4).asInstanceOf[Int], x(5).toString))
 var dfg = spark.createDataFrame(t21).toDF("cvbn","dfghj","fghj","fghj","dfgh","456")
    dfg.show()


    var partitioncols= ListBuffer("IPMTDT","INT_PMT_DATE","DEAL_ID")
    var values1 = ListBuffer(List("YEAR","IPMTDT","MM/dd/yyyy","year"),List("Mnoth","IPMTDT","MM/dd/yyyy","month"),List("DEAL_ID","DEAL_ID","None","none"))
    var values2 = ("Mnoth","IPMTDT","MM/dd/yyyy","month")
    var values3 =("DEAL_ID","DEAL_ID","None","none")
    var derived_col = ListBuffer("YEAR","MONTH","DEAL")
    var base_col = ListBuffer("IPMTDT","INT_PMT_DATE","DEAL_ID")
    var base_format = ListBuffer("MM/dd/yyyy","mm/dd/yy 0:00","STRING")
    var derived_exp = ListBuffer("year","month","STRING")
    var m =  ListBuffer[List[String]]()
    import spark.implicits._
    //var result_df_list = new ListBuffer[DataFrame]()
    //var df13 = Seq[Seq[AnyVal]]
    //df13=df13 :+ Seq(values2)
    //println(df13)
    val tom = TestPerson("Mnoth","IPMTDT","MM/dd/yyyy","month")
    val sam = TestPerson("DEAL_ID","DEAL_ID","None","none")
    val PersonList = mutable.MutableList[TestPerson]()
    PersonList += tom
    PersonList += sam
 println(PersonList)
    val personDS = PersonList.toDS()
    println(personDS.getClass)
    personDS.show()

    val personDF = PersonList.toDF()
    println(personDF.getClass)
    personDF.show()
    personDF.select("name", "age").show()

   // m ++=List(values2)
    //m ++= List(values3)
    println(m)
    val df12 = m.toDF("Der","base","format","desire")
    df12.show()

   /*for(i<-0 to partitioncols.size-1){
     var colname :String=partitioncols(i)
     var values :List[String] = values1(i)
     m  += (colname,values)
    }*/
    println("mapping " +m)
    //var testDF = spark.emptyDataFrame("partitioncols")
    import spark.implicits._

    val columns = Seq("name","address")
    val data = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"),
      ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))
    var dfFromData = spark.createDataFrame(data).toDF(columns:_*)
    dfFromData.printSchema()
    dfFromData.show()

    /*val schema = StructType(
        StructField("Derived_col", StringType, true) ::
        StructField("Base_col", StringType, false) ::
        StructField("base_format", StringType, false) ::
        StructField("desired_format", StringType, false) ::
        StructField("desired_datatype", StringType, false) ::
        StructField("part_order", IntegerType, false) ::
    )*/



     var ParLS= ListBuffer(
       ("YEAR","IPMTDT","MM/dd/yyyy","year","TimeStamp",2),
       ("REGION","REGION","None","None","String",5),
       ("MONTH","IPMTDT","MM/dd/yyyy","month","TimeStamp",3),
       ("DEAL_ID","DEAL_ID","None","None","String",4)
     )
    var partdf1=ParLS.toDF("Derived_col", "Base_col", "base_format","desired_format","desired_datatype","part_order")
    partdf1.show()
    var partdf = spark.createDataFrame(Seq(
      ("YEAR","IPMTDT","MM/dd/yyyy","year","TimeStamp",2),
      ("REGION","REGION","None","None","String",5),
      ("MONTH","IPMTDT","MM/dd/yyyy","month","TimeStamp",3),
      ("DEAL_ID","DEAL_ID","None","None","String",4)
    )).toDF("Derived_col", "Base_col", "base_format","desired_format","desired_datatype","part_order")

   partdf.show()
    println(partdf.count())
    var fg = partdf.orderBy("part_order")

    val patitioncol = partdf.select("Derived_col")
    patitioncol.show()
    var separt= ListBuffer("Batch_ID")
    val sdf=fg.select("Derived_col").map(r => r.getString(0)).collect.toList
   separt ++= sdf
    println(separt)
    print(partdf.select($"Derived_col" <=> $"Base_col").show())
    var df2 = df
    for (row <- partdf.rdd.collect)
    {
      var newcol = row.mkString(",").split(",")(0)
      var base = row.mkString(",").split(",")(1)
      var baseformat = row.mkString(",").split(",")(2)
      var desformat = row.mkString(",").split(",")(3)
      if(newcol != base){
        if(desformat == "year") df2 = df2.withColumn(newcol,year(to_date(df((base )),baseformat)))
        if(desformat =="month") df2 = df2.withColumn(newcol,month(to_date(df((base )),baseformat)))
      }
      println(newcol+" " + base)
    }
    df2.show()
    def get_partcols(df: DataFrame,partitioncols:Seq[String],partdf: DataFrame)={
      //var newDF = df
      var df3 = df
      var sortedDF = partdf.orderBy("part_order")
      var newpartcols=ListBuffer("BATCH_ID")
      val sdf=sortedDF.select("Derived_col").map(r => r.getString(0)).collect.toList
      newpartcols ++= sdf
      for (row <- partdf.rdd.collect)
      {
        var newcol = row.mkString(",").split(",")(0)
        var base = row.mkString(",").split(",")(1)
        var baseformat = row.mkString(",").split(",")(2)
        var desformat = row.mkString(",").split(",")(3)
        if(newcol != base){
          if(desformat == "year") df3 = df3.withColumn(newcol,year(to_date(df((base )),baseformat)))
          if(desformat =="month") df3 = df3.withColumn(newcol,month(to_date(df((base )),baseformat)))
        }
        //println(newcol+" " + base)
      }
      /*for(i<-0 to partitioncols.size-2){
        var colname=partitioncols(i)
        /*newDF.schema(colname).dataType match {
          case StringType=>  newDF=newDF.withColumn("timestampcol", unix_timestamp(newDF(colname), "MM/dd/yyyy").cast(TimestampType))
        }*/

       // newDF=newDF.withColumn("timestampcol", to_date(newDF(colname), "MM/dd/yyyy"))
        if(to_date(newDF(colname), "MM/dd/yyyy") != false){
          newDF=newDF.withColumn("timestampcol", to_date(newDF(colname), "MM/dd/yyyy"))
        }
        //case (to_date(newDF(colname), "MM/dd/yyyy").isNotNull) =>newDF=newDF.withColumn("timestampcol", to_date(newDF(colname), "MM/dd/yyyy")

        newDF= newDF.withColumn("Year", year(newDF("timestampcol")).cast("String"))

         newpartcols=newpartcols :+"Year" //+"Month"
        newDF = newDF.drop(newDF.col("timestampcol"))
      }*/
      (df2,newpartcols)
    }
    var (df1,seqpart) = get_partcols( df,partitioncols, partdf)
    println(seqpart)
    df1.show()
    df1.printSchema()

     
     
    //df.printSchema()
    //val resDF = df.withColumn( "timestamp",unix_timestamp(df("IPMTDT"), "MM/dd/yyyy").cast(TimestampType))
    //resDF.show()
    //val tempDF= resDF.withColumn("Year", year(resDF("timestamp"))).withColumn("Month",month(resDF("timestamp")))
    //tempDF.show()
    //val tyes = df.schema("IPMTDT").dataType
    //if(tyes==StringType){
     // println(tyes)
    //}

  }
}

