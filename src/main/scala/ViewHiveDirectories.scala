package com.km.scala

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/* 
//Execution:
principal="km@AD.mycompCOM"
pr_keytab="/home/km/.km.keytab"

spark2-submit --name "View Hive Directories" \
      --class com.km.scala.ViewHiveDirectories \
      --master yarn --deploy-mode client \
      --driver-class-path /etc/spark2/conf:/etc/hive/conf \
      --queue general \
      --num-executors 4 --executor-cores 2 \
      --conf spark.port.maxRetries=100 --conf spark.ui.port=4061 \
      --conf spark.yarn.principal=${principal} \
      --conf spark.yarn.keytab=${pr_keytab} \
   ./km_scala2_2.11-0.1.0.jar "mydb" "searchstring" >> km_view_hive_dir_2019_04_23_epsi_pdw.log
   
 */
object ViewHiveDirectories {
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info(getDT() + "; Class name=" + this.getClass.getName )

    //input arguments
    if(args.size < 1){
      println("Please verify input arguments:");
      println("0: Database name")
      println("1: (Optional) Table Name Search String")
      System.exit(99)
    }
    val sDBName = args(0);
    println(getDT() + ": arg(0): DB Name: " + sDBName);
    var sSearchString = "";
    try{
      sSearchString=args(1);
    }catch{
      case e: Exception => println(s"------- *** Table search argument is not passed  ***");
        println("------- *** -> " + e);
    }
    println(getDT() + ": arg(1): Search String: " + sSearchString);

    val conf = new SparkConf()
      .setMaster("yarn") //yarn
      .setAppName("View Hive Directories")
    //conf.set("spark.testing.memory", "2147480000")

    val spark = SparkSession.builder
      .config(conf)
      .enableHiveSupport()
      .getOrCreate
/*
    val spark = SparkSession.builder()
      .appName("View Hive Directories")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()
  */
    spark.sparkContext.setLogLevel("ERROR")

    try {
      import org.apache.spark.sql.functions._
      import spark.implicits._

      import org.apache.spark.sql.functions.{concat, lit}

        var df_tbls = spark.sql(s"show tables in $sDBName").
          filter($"tablename".contains(sSearchString)).
        withColumn("table_name", concat($"database", lit("."), $"tableName")).
        drop("isTemporary", "database", "tableName")

      //val df_tbls=Seq(("vivid.km_ext")).toDF("table_name")
      //df_tbls.count
      //=>61
      df_tbls.show(3, false)

      //Get Locations of each table
      //spark.sql("desc formatted vivid.epsi_2016_largesample").filter($"col_name"==="Location").show(false)

      var tablesMap = df_tbls.map(rec => (rec.getString(0), "")).rdd.groupByKey.mapValues(_.mkString(",")).collectAsMap
      //tablesMap.size => 61
      //tablesMap.map(v => (v._1, v._2)).foreach(println)

      var locMapMtb = collection.mutable.Map(tablesMap.toSeq: _*)
      println("Map items total: " + locMapMtb.size)
      //locMapMtb.foreach(println)

      //Get location for each
      val locMapMtbKeys = locMapMtb.keys
      locMapMtbKeys.foreach((dbName) => {
        println(s"----- $dbName");
        try {
          val df_tbl_loc = spark.sql(s"describe formatted $dbName").filter($"col_name" === "Location").drop("col_name", "comment");
          //val df_tbl_loc = Seq(("hdfs:///xxx")).toDF("data_type")
          if (df_tbl_loc.count > 0) {
            val sLoc = df_tbl_loc.map(rec => rec.getString(0)).collect().mkString(",");
            println(s"Locations for $dbName : " + sLoc);
            locMapMtb.update(dbName, sLoc);
          }
        }catch{
          case e: Exception => println(s"------- *** Exception occured: Key: $dbName ***")
            println("------- *** -> " + e);
          locMapMtb.remove(dbName);
        }
      })
      println("Map items total: " + locMapMtb.size)
      //locMapMtb.foreach(println)

      println("----- Hive table locations ------")
      val df_tbl_loc = locMapMtb.toSeq.toDF("table_name", "location")
      df_tbl_loc.show(false)
      println("-----------")

      var prtnsMapMtb = collection.mutable.Map(tablesMap.toSeq: _*)
      //prtnsMapMtb.foreach(println)

      //Get partition for each
      val prtnsMapMtbKeys = locMapMtb.keys
      prtnsMapMtbKeys.foreach((dbName) => {
        println(s"----- $dbName");
        try{
          val df_tbl_ptn = spark.sql(s"show partitions $dbName");
          //val df_tbl_ptn = Seq(("extract_date=xxx")).toDF()
          //val df_tbl_ptn = Seq(("prtn=value"))
          if (df_tbl_ptn.count > 0) {
            val sLoc = df_tbl_ptn.map(rec => rec.getString(0)).collect().mkString(",");
            println(s"Locations for $dbName : " + sLoc);
            prtnsMapMtb.update(dbName, sLoc);
          }
        }catch{
        case e: Exception => println(s"------- *** Exception occured: Key: $dbName  ***");
            println("------- *** -> " + e);
      }
      })
      println("Map items total: " + locMapMtb.size)
      //prtnsMapMtb.foreach(println)

      println("----- Hive table Partitions ------")
      val df_tbl_prtn = prtnsMapMtb.toSeq.toDF("table_name", "partition")
      df_tbl_prtn.show(false)
      println("---------------------------")

      val df_tbls_dtls = df_tbls.join(df_tbl_loc, Seq("table_name")).join(df_tbl_prtn, Seq("table_name"))
      //.withColumn("run_time", lit(new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss").format(new Date())))
      df_tbls_dtls.show(100, false)

      df_tbls_dtls.registerTempTable("df_tbls_dtls")
      //var run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      val run_time_ms = System.currentTimeMillis()
      println("ts: " + run_time_ms)
      //spark.sql("set hive.exec.dynamic.partition=true")
      spark.sql(s"insert into vivid.data_tbls_search partition (run_time='$run_time_ms') select * from df_tbls_dtls")

      println("------COMPLETED--------")
    }catch{
      //Log exception
      case e: Exception => println("*************  Exception occured ************ ");
        println(e);
    }finally {
      spark.close()
    }
  }

  def getDT()
  : String = {

    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
    return dateFormatter.format(new Date())

    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)
  }
}
