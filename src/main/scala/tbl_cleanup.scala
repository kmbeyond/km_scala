package com.wp.da.cs

import java.text.SimpleDateFormat
import java.util.Date
//import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object tbl_cleanup {
  //val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    //logger.info(getDT() + "; Class name=" + this.getClass.getName )

    //try {
      //input arguments
    if (args.size < 3) {
      println("Please verify input arguments:");
      println("0: Database name (Ex: mydb)")
      println("1: HDFS Path to save to disk")
      println("2: Days back to cleanup")
      System.exit(101)
    }

    val sDBName = args(0);
    println(getDTFormat() + ": ***** arg(0): " + sDBName);
    val inputHDFSPath = args(1);
    println(getDTFormat() + ": ***** arg(1): " + inputHDFSPath);
    val iDays = args(2).toInt
    println(getDTFormat() + ": ***** arg(2): " + iDays);

      //val conf = new SparkConf()
        //.setMaster("yarn") //yarn
        //.setAppName("Commerce Signals")

      val spark = SparkSession.builder
        .appName("CS Tables Data Cleanup")
        .master("yarn")
        .enableHiveSupport()
        .getOrCreate()
    //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    //.config("spark.hadoop.validateOutputSpecs", "false")
    //.config("spark.debug.maxToStringFields", 100)
     spark.sparkContext.setLogLevel("ERROR")

      import org.apache.spark.sql.functions._
      //import spark.implicits._

/*
    import org.apache.spark.sql.{Row, SQLContext, SaveMode}
    import org.apache.spark.sql.functions.{col, to_date, trim, udf, unix_timestamp, _}
    //import org.apache.spark.sql.types.{StringType, StructField, StructType}
    import org.apache.spark.{SparkConf, SparkContext}

    val conf = new SparkConf().setAppName("KM Test")
    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    //import sqlContext.implicits._
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)
    //hc.sql("set spark.sql.caseSensitive=false")
    //hc.sql("set hive.execution.engine=spark")
    val spark = hc
    //spark.tables("vivid").show()
    //spark.tables().show()
*/
    import spark.implicits._

    //spark.sql("show databases").show(false)
    //spark.conf.getAll.foreach(println)

    //val proc_st_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())

    var run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"insert into table ${sDBName}.cs_logs select '${run_time_ts}','cs','099','',0,'Hive table data merge START.'")

    //if (new scala.util.Random().nextInt(10) % 4 == 0) {
      var sTableName = ""
      var sTmpDirName = ""
      println(getDTFormat()+": ----------- Merge data files: START -------------")
      //-------------001: .cs_request--------------
      sTableName = s"$sDBName.cs_request"
      sTmpDirName = s"${inputHDFSPath}/tmp_cs_request"
      spark.table(sTableName).filter($"process_ts" >= date_sub(current_timestamp(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)

      //spark.table(sTableName).write.saveAsTable(s"${sTableName}_tmp")
      //spark.table(s"${sTableName}_tmp").repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.sql(s"drop table ${sTableName}_tmp")

      //-------------002: cs_request_merchant--------------
      sTableName = s"$sDBName.cs_request_merchant"
      sTmpDirName = s"${inputHDFSPath}/tmp_cs_request_merchant"
      spark.table(sTableName).filter($"process_ts" >= date_sub(current_timestamp(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.table(sTableName).write.saveAsTable(s"${sTableName}_tmp")
      //spark.table(s"${sTableName}_tmp").repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.sql(s"drop table ${sTableName}_tmp")

      //-------------003: cs_request_audience--------------
      sTableName = s"$sDBName.cs_request_audience"
      sTmpDirName = s"${inputHDFSPath}/tmp_cs_request_audience"
      spark.table(sTableName).filter($"process_ts" >= date_sub(current_timestamp(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.table(sTableName).write.saveAsTable(s"${sTableName}_tmp")
      //spark.table(s"${sTableName}_tmp").repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.sql(s"drop table ${sTableName}_tmp")

      //------------004: cs_audience --------------
      //try{
      //  spark.sql(s"ALTER TABLE $sDBName.cs_audience DROP IF EXISTS partition (process_date='"+(java.time.LocalDate.now).minusDays(4)+"')");
      //}catch{
      //  case ex: Exception => { println(getDTFormat()+": Exception to drop partition "+(java.time.LocalDate.now).minusDays(4)+s" on $sDBName.cs_audience"); }
      //}
      //sTableName = s"$sDBName.cs_audience"
      //sTmpDirName = s"${inputHDFSPath}/tmp_cs_audience"
      //spark.sql(s"select process_date,count(1) as cnt_rec from ${sTableName} GROUP by process_date").show(30,false)
      //spark.table(sTableName).select("process_date").filter($"process_date" >= date_sub(current_date(), 20)).distinct.show()
      //spark.table(sTableName).filter($"process_date" >= date_sub(current_date(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      //spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.sql(s"refresh table ${sTableName}")

      //-------------005: cs_response_data--------------
      sTableName = s"$sDBName.cs_response_data"
      sTmpDirName = s"${inputHDFSPath}/tmp_cs_response_data"
      spark.table(sTableName).filter($"process_ts" >= date_sub(current_timestamp(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.table(sTableName).write.saveAsTable(s"${sTableName}_tmp")
      //spark.table(s"${sTableName}_tmp").repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.sql(s"drop table ${sTableName}_tmp")

      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table ${sDBName}.cs_logs select '${run_time_ts}','cs','099','',0,'Hive table data merge completed.'")

      println(getDTFormat()+": ----------- Merge data files: COMPLETE -------------")
    //}

    //println(getDTFormat()+": REFRESH TABLE ")
    //spark.sql(s"refresh table ${sDBName}.cs_logs")
    //spark.sql(s"refresh table ${sDBName}.cs_request")
    //spark.sql(s"refresh table ${sDBName}.cs_request_merchant")
    //spark.sql(s"refresh table ${sDBName}.cs_request_audience")
    //spark.sql(s"refresh table ${sDBName}.cs_audience")
    //spark.sql(s"refresh table ${sDBName}.cs_response_data")

    println(getDTFormat() + ": Completed. Exiting spark job.");
  }//end main()


  def getDT() : String = {
    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    return dateFormatter.format(new Date())
    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)
  }
  def getDTFormat(): String = {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(new Date())
  }
}
