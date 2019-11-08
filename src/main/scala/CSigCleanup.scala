package com.wp.da.csig

import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
//import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object CSigCleanup {
  //val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    //logger.info(getDT() + "; Class name=" + this.getClass.getName )
    println(getDTFormat() + ": ********** CSigCleanup **********");
    try {
      //input arguments
    if (args.size < 3) {
      println("Please verify input arguments:");
      println("0: Database name (Ex: data_comm)")
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

      val spark = SparkSession.builder
        .appName("Commerce Signals Data Cleanup")
        .master("yarn")
        .enableHiveSupport()
        .getOrCreate()

     spark.sparkContext.setLogLevel("ERROR")

      import sys.process._
      import org.apache.spark.sql.functions._
      import spark.implicits._

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

    //spark.conf.getAll.foreach(println)

    //val proc_st_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())

    var run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"insert into table ${sDBName}.csig_logs select '${run_time_ts}','CSIG','099','',0,'Hive table data merge START.'")

    //if (new scala.util.Random().nextInt(10) % 4 == 0) {
      var sTableName = ""
      var sTmpDirName = ""
      println(getDTFormat()+": ----------- Merge/clean-up data files: START -------------")
      //-------------001: .csig_request--------------
      sTableName = s"$sDBName.csig_request"
      sTmpDirName = s"${inputHDFSPath}/tmp_csig_request"
      spark.table(sTableName).filter($"process_ts" >= date_sub(current_timestamp(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)

      //spark.table(sTableName).write.saveAsTable(s"${sTableName}_tmp")
      //spark.table(s"${sTableName}_tmp").repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.sql(s"drop table ${sTableName}_tmp")

      //-------------002: csig_request_merchant--------------
      sTableName = s"$sDBName.csig_request_merchant"
      sTmpDirName = s"${inputHDFSPath}/tmp_csig_request_merchant"
      spark.table(sTableName).filter($"process_ts" >= date_sub(current_timestamp(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.table(sTableName).write.saveAsTable(s"${sTableName}_tmp")
      //spark.table(s"${sTableName}_tmp").repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.sql(s"drop table ${sTableName}_tmp")

      //-------------003: csig_request_audience--------------
      sTableName = s"$sDBName.csig_request_audience"
      sTmpDirName = s"${inputHDFSPath}/tmp_csig_request_audience"
      spark.table(sTableName).filter($"process_ts" >= date_sub(current_timestamp(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.table(sTableName).write.saveAsTable(s"${sTableName}_tmp")
      //spark.table(s"${sTableName}_tmp").repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.sql(s"drop table ${sTableName}_tmp")

      //------------004: csig_audience --------------
      //try{
      //  spark.sql(s"ALTER TABLE $sDBName.csig_audience DROP IF EXISTS partition (process_date='"+(java.time.LocalDate.now).minusDays(4)+"')");
      //}catch{
      //  case ex: Exception => { println(getDTFormat()+": Exception to drop partition "+(java.time.LocalDate.now).minusDays(4)+s" on $sDBName.csig_audience"); }
      //}
      //sTableName = s"$sDBName.csig_audience"
      //sTmpDirName = s"${inputHDFSPath}/tmp_csig_audience"
      //spark.sql(s"select process_date,count(1) as cnt_rec from ${sTableName} GROUP by process_date").show(30,false)
      //spark.table(sTableName).select("process_date").filter($"process_date" >= date_sub(current_date(), 20)).distinct.show()
      //spark.table(sTableName).filter($"process_date" >= date_sub(current_date(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      //spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.sql(s"refresh table ${sTableName}")

      var iDays2=iDays+20
      var cal = java.util.Calendar.getInstance();
      cal.add(Calendar.DATE, -iDays2)
      var dtyyyyMM = new java.text.SimpleDateFormat("yyyy-MM").format(new java.util.Date(cal.getTimeInMillis()))
      s"hdfs dfs -ls -t $inputHDFSPath/csig_audience/process_date=$dtyyyyMM*".!
      s"hdfs dfs -rm -R $inputHDFSPath/csig_audience/process_date=$dtyyyyMM*".!

      iDays2=iDays2+30
      cal = java.util.Calendar.getInstance();
      cal.add(Calendar.DATE, -iDays2)
      dtyyyyMM = new java.text.SimpleDateFormat("yyyy-MM").format(new java.util.Date(cal.getTimeInMillis()))
      s"hdfs dfs -ls -t $inputHDFSPath/csig_audience/process_date=$dtyyyyMM*".!
      s"hdfs dfs -rm -R $inputHDFSPath/csig_audience/process_date=$dtyyyyMM*".!

      iDays2=iDays2+30
      cal = java.util.Calendar.getInstance();
      cal.add(Calendar.DATE, -iDays2)
      dtyyyyMM = new java.text.SimpleDateFormat("yyyy-MM").format(new java.util.Date(cal.getTimeInMillis()))
      s"hdfs dfs -ls -t $inputHDFSPath/csig_audience/process_date=$dtyyyyMM*".!
      s"hdfs dfs -rm -R $inputHDFSPath/csig_audience/process_date=$dtyyyyMM*".!

      try{
        spark.sql(s"ALTER TABLE $sDBName.csig_audience DROP IF EXISTS partition (process_date='"+(java.time.LocalDate.now).minusDays(iDays+10)+"')");
        spark.sql(s"ALTER TABLE $sDBName.csig_audience DROP IF EXISTS partition (process_date='"+(java.time.LocalDate.now).minusDays(iDays+11)+"')");
        spark.sql(s"ALTER TABLE $sDBName.csig_audience DROP IF EXISTS partition (process_date='"+(java.time.LocalDate.now).minusDays(iDays+12)+"')");
        spark.sql(s"ALTER TABLE $sDBName.csig_audience DROP IF EXISTS partition (process_date='"+(java.time.LocalDate.now).minusDays(iDays+13)+"')");
        spark.sql(s"ALTER TABLE $sDBName.csig_audience DROP IF EXISTS partition (process_date='"+(java.time.LocalDate.now).minusDays(iDays+14)+"')");
        spark.sql(s"ALTER TABLE $sDBName.csig_audience DROP IF EXISTS partition (process_date='"+(java.time.LocalDate.now).minusDays(iDays+15)+"')");
        spark.sql(s"ALTER TABLE $sDBName.csig_audience DROP IF EXISTS partition (process_date='"+(java.time.LocalDate.now).minusDays(iDays+16)+"')");

      }catch{
          case ex: Exception => { println(getDTFormat()+ s": Exception to drop partition(s) on $sDBName.csig_audience"); }
      }

      //-------------005: csig_response_data--------------
      sTableName = s"$sDBName.csig_response_data"
      sTmpDirName = s"${inputHDFSPath}/tmp_csig_response_data"
      spark.table(sTableName).filter($"process_ts" >= date_sub(current_timestamp(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.table(sTableName).write.saveAsTable(s"${sTableName}_tmp")
      //spark.table(s"${sTableName}_tmp").repartition(1).write.mode("overwrite").insertInto(sTableName)
      //spark.sql(s"drop table ${sTableName}_tmp")


      //-------------010: .csig_cg_request--------------
      sTableName = s"$sDBName.csig_cg_request"
      sTmpDirName = s"${inputHDFSPath}/tmp_csig_cg_request"
      spark.table(sTableName).filter($"process_ts" >= date_sub(current_timestamp(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)


      //-------------011: csig_cg_request_audience--------------
      sTableName = s"$sDBName.csig_cg_request_audience"
      sTmpDirName = s"${inputHDFSPath}/tmp_csig_cg_request_audience"
      spark.table(sTableName).filter($"process_ts" >= date_sub(current_timestamp(), iDays)).write.mode("overwrite").parquet(sTmpDirName)
      spark.read.parquet(sTmpDirName).repartition(1).write.mode("overwrite").insertInto(sTableName)

      //------------012: csig_cg_audience --------------

      iDays2=iDays+20
      cal = java.util.Calendar.getInstance();
      cal.add(Calendar.DATE, -iDays2)
      dtyyyyMM = new java.text.SimpleDateFormat("yyyy-MM").format(new java.util.Date(cal.getTimeInMillis()))
      s"hdfs dfs -ls -t $inputHDFSPath/csig_cg_audience/process_date=$dtyyyyMM*".!
      s"hdfs dfs -rm -R $inputHDFSPath/csig_cg_audience/process_date=$dtyyyyMM*".!

      iDays2=iDays2+30
      cal = java.util.Calendar.getInstance();
      cal.add(Calendar.DATE, -iDays2)
      dtyyyyMM = new java.text.SimpleDateFormat("yyyy-MM").format(new java.util.Date(cal.getTimeInMillis()))
      s"hdfs dfs -ls -t $inputHDFSPath/csig_cg_audience/process_date=$dtyyyyMM*".!
      s"hdfs dfs -rm -R $inputHDFSPath/csig_cg_audience/process_date=$dtyyyyMM*".!

      iDays2=iDays2+30
      cal = java.util.Calendar.getInstance();
      cal.add(Calendar.DATE, -iDays2)
      dtyyyyMM = new java.text.SimpleDateFormat("yyyy-MM").format(new java.util.Date(cal.getTimeInMillis()))
      s"hdfs dfs -ls -t $inputHDFSPath/csig_cg_audience/process_date=$dtyyyyMM*".!
      s"hdfs dfs -rm -R $inputHDFSPath/csig_cg_audience/process_date=$dtyyyyMM*".!


      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table ${sDBName}.csig_logs select '${run_time_ts}','CSIG','099','',0,'Hive table data merge/cleanup completed.'")

      println(getDTFormat()+": ----------- Merge/cleanup data files: COMPLETE -------------")
    }catch{
      case ex: Exception => { println("---Exception while processing: " + ex) }
      case _: Exception => { println("---Exception while processing--") }
    }

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
