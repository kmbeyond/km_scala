package com.wp.da.csig

import java.text.SimpleDateFormat
import java.util.Date
//import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CSigProcess {
  //val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    //logger.info(getDT() + "; Class name=" + this.getClass.getName )
    println(getDTFormat() + ": ********** CSigProcess **********");
    //try {
    //input arguments
    if (args.size < 2) {
      println("Please verify input arguments:");
      println("0: Database name (Ex: dev_data_comm)")
      println("1: HDFS Path for JSON & audience files")
      System.exit(101)
    }

    val sDBName = args(0);
    println(getDTFormat() + ": ***** arg(0): " + sDBName);
    val inputHDFSPath = args(1);
    println(getDTFormat() + ": ***** arg(1): " + inputHDFSPath);

    val spark = SparkSession.builder
      .appName("Commerce Signals Process JSON")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import org.apache.spark.sql.functions._
    import sys.process._
    import spark.implicits._

    val proc_st_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())

    var run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts', 'CSIG', '002', 'START', 0, 'JSON load Process Started'")

    println(getDT() + s":------ Reading JSON: ${inputHDFSPath} --------")
    val csigReq = spark.read.json(inputHDFSPath + "/incoming/measurement/*.jsn") // + inputJsonFile);
    //csigReq.show(false)

    var csigReqColumns = csigReq.columns
    if (csigReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ":-------------- json data is invalid ------------------")
      System.exit(102)
    }

    //-------------001: .csig_request--------------
    val df_csig_request = csigReq.withColumn("dateRanges", explode($"dateRanges")).
      withColumn("description", $"dateRanges.description").
      withColumn("start_date", $"dateRanges.startDate").
      withColumn("end_date", $"dateRanges.endDate").
      //withColumn("time_zone", $"dateRanges.timeZoneId").
      withColumn("transaction_amount", when(array_contains($"metrics", "transactionAmount"), "Y").otherwise("")).
      withColumn("transaction_count", when(array_contains($"metrics", "transactionCount"), "Y").otherwise("")).
      withColumn("matched_ids_count", when(array_contains($"metrics", "matchedIdsCount"), "Y").otherwise("")).
      withColumn("transactor_count", when(array_contains($"metrics", "transactorCount"), "Y").otherwise("")).
      withColumn("stddev_spend_txn", when(array_contains($"metrics", "stdDevSpendByTxn"), "Y").otherwise("")).
      withColumn("stddev_spend_transactor", when(array_contains($"metrics", "stdDevSpendByTransactor"), "Y").otherwise("")).
      withColumn("stddev_txncnt_transactor", when(array_contains($"metrics", "stdDevTxnCntByTransactor"), "Y").otherwise("")).
      withColumn("request_date", lit(java.time.LocalDate.now.toString)).
      select($"requestid".as("request_id"),lit(proc_st_ts).as("process_ts"),lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts"), $"description", $"start_date", $"end_date" //, $"time_zone"
        , $"request_date", $"transaction_amount", $"transaction_count", $"matched_ids_count", $"transactor_count", $"stddev_spend_txn", $"stddev_spend_transactor", $"stddev_txncnt_transactor")

    csigReqColumns = df_csig_request.columns
    if (csigReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ":-------------- json data is invalid: df_csig_request ------------------")
      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts', 'CSIG', '002_01', 'csig_request', 1, 'Corrupt JSON data file'")
      System.exit(102)
    }
    println(getDTFormat() + s": --> ${sDBName}.csig_request")
    df_csig_request.coalesce(1).write.insertInto(sDBName+".csig_request")
    //nodf_csig_request.createOrReplaceTempView("df_csig_request")
    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    //nospark.sql(s"insert into $sDBName.csig_request select request_id,'$proc_st_ts','$run_time_ts',description,start_date,end_date,time_zone,request_date,transaction_amount,transaction_count,matched_ids_count,transactor_count,stddev_spend_txn,stddev_spend_transactor,stddev_txncnt_transactor FROM df_csig_request")
    println(getDTFormat() + s": Inserted in ${sDBName}.csig_request");
    spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts','CSIG','002_01','csig_request', 0, 'Inserted requests: '")

    //-------------002: csig_request_merchant--------------
    val df_csig_request_merchant = csigReq.select($"requestid".as("request_id"),lit(proc_st_ts).as("process_ts"),lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts"), concat_ws(",", $"merchants").as("merchants"), explode($"merchants").as("merchant_id"))
    csigReqColumns = df_csig_request_merchant.columns
    if (csigReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ": -------------- json data is invalid: df_csig_request_merchant ------------------");
      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts', 'CSIG', '002_02', 'csig_request_merchant', 1, 'Corrupt JSON data file'")
      System.exit(102)
    }
    println(getDTFormat() + s": --> ${sDBName}.csig_request_merchant")
    df_csig_request_merchant.coalesce(1).write.insertInto(sDBName+".csig_request_merchant")
    //nodf_csig_request_merchant.createOrReplaceTempView("df_csig_request_merchant")
    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    //nospark.sql(s"INSERT into $sDBName.csig_request_merchant SELECT request_id,'$proc_st_ts','$run_time_ts',merchants,merchant_id FROM df_csig_request_merchant")
    println(getDTFormat() + s": Inserted in ${sDBName}.csig_request_merchant");
    spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts','CSIG','002_02','csig_request_merchant',0,'Inserted Request Merchants'")

    //-------------003: csig_request_audience--------------
    val df_csig_request_audience_exp = csigReq.select($"requestid".as("request_id"), explode($"audiences").as("audiences")) //.
      //withColumn("request_id", $"request_id").
      //withColumn("audience_id", $"audiences.id").
      //withColumn("uri", $"audiences.uri").
      //withColumn("source", $"audiences.source").
      //withColumn("count", $"audiences.count")

    csigReqColumns = df_csig_request_audience_exp.columns
    if (csigReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ":-------------- json data is invalid: csig_request_audience ------------------")
      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts','CSIG','002_03','csig_request_audience',1,'Corrupt JSON data file'")
      System.exit(102)
    }
    //val df_csig_request_audience = df_csig_request_audience_exp.select($"request_id", lit(proc_st_ts).as("process_ts"),lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts"), $"audience_id", $"uri", $"source", $"count")
    val df_csig_request_audience = df_csig_request_audience_exp.select($"request_id",
      lit(proc_st_ts).as("process_ts"),lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts"),
      $"audiences.id".as("audience_id"), $"audiences.uri", $"audiences.source", $"audiences.count")
    println(getDTFormat() + s": --> ${sDBName}.csig_request_audience")
    df_csig_request_audience.coalesce(1).write.insertInto(sDBName+".csig_request_audience")
    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    //nodf_csig_request_audience.createOrReplaceTempView("df_csig_request_audience")
    //nospark.sql(s"insert into $sDBName.csig_request_audience select request_id,'$proc_st_ts','$run_time_ts',audiences FROM df_csig_request_audience")
    println(getDTFormat() + s": Inserted in ${sDBName}.csig_request_audience");
    spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts','CSIG','002_03','csig_request_audience',0,'Inserted Request Audiences'")

    //-------------004: csig_audience--------------
    def funFileName: ((String) => String) = { (s) =>(s.split("/").last)}
    import org.apache.spark.sql.functions.udf
    val udfFileName = udf(funFileName)

    //var df_audience_data=spark.read.csv(inputHDFSPath+ "/incoming/measurement/*.gz").toDF("cs_sha").withColumn("uri", udfFileName(input_file_name()))
    var df_audience_data=spark.read.csv(inputHDFSPath+ "/incoming/measurement/*.gz").toDF("cs_sha").withColumn("uri", udfFileName(input_file_name())).
      join(df_csig_request_audience, Seq("uri"), "left_outer")
    df_audience_data.cache()
    val df_audience=df_audience_data.   // df_csig_request_audience.select("uri","request_id","audience_id").join(df_audience_data, Seq("uri")).
      select($"request_id"
        , lit(proc_st_ts).as("process_ts")
        , lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts")
        , $"audience_id", $"cs_sha", lit("").as("impression_window")
      )
    df_audience.createOrReplaceTempView("df_audience");
    println(getDTFormat() + s": --> ${sDBName}.csig_audience")
    spark.sql(s"INSERT into $sDBName.csig_audience partition (process_date='" + (java.time.LocalDate.now) + s"') SELECT * from df_audience");
    println(getDTFormat() + s": Inserted in ${sDBName}.csig_audience");

    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"INSERT INTO TABLE $sDBName.csig_logs SELECT '$run_time_ts','CSIG','002_04','csig_audience',0,'Inserted Audience data'")

    //--- Create Aud summary file - START
    df_audience_data.createOrReplaceTempView("df_audience_data")
    //val df_audience_files=df_csig_request_audience.select("uri","request_id").join(df_audience_data, Seq("uri"), "left_outer").
    //  select($"request_id",$"cs_sha", $"uri"  ).groupBy("request_id","uri").agg(sum(when($"cs_sha".isNull,0).otherwise(1)).alias("aud_count"))

    var sCurrDt = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new java.util.Date())
    spark.sql("SELECT request_id,uri,count(request_id) AS aud_count FROM df_audience_data WHERE request_id IS NULL GROUP BY request_id,uri").
      coalesce(1).write.options(Map("header"->"True")).csv(s"${inputHDFSPath}/outgoing/measurement/${sCurrDt}")
    s"hdfs dfs -mv ${inputHDFSPath}/outgoing/measurement/${sCurrDt}/*.csv ${inputHDFSPath}/outgoing/measurement/CSIG_aud_data_miss_${sCurrDt}.csv".!
    println(getDTFormat() + s": --> Audience Summary file: ${inputHDFSPath}/outgoing/measurement/CSIG_aud_summary_${sCurrDt}.csv")
    s"hdfs dfs -rm -R $inputHDFSPath/outgoing/measurement/$sCurrDt".!

    sCurrDt = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new java.util.Date())
    spark.sql("SELECT request_id,uri,count(request_id) AS aud_count FROM df_audience_data GROUP BY request_id,uri").
      coalesce(1).write.options(Map("header"->"True")).csv(s"${inputHDFSPath}/outgoing/measurement/${sCurrDt}")
    s"hdfs dfs -mv ${inputHDFSPath}/outgoing/measurement/${sCurrDt}/*.csv ${inputHDFSPath}/outgoing/measurement/CSIG_aud_summary_${sCurrDt}.csv".!
    println(getDTFormat() + s": --> Audience Summary file: ${inputHDFSPath}/outgoing/measurement/CSIG_aud_summary_${sCurrDt}.csv")
    s"hdfs dfs -rm -R $inputHDFSPath/outgoing/measurement/$sCurrDt".!

    //--- Create Aud summary file - END

    //------------Populate output data
    //Moved to beeline command

    //-------------006: csig_response_data--------------
    //Moved to new class file

    if (new scala.util.Random().nextInt(10) % 3 == 0) {
      com.wp.da.csig.CSigCleanup.main(Array(sDBName, inputHDFSPath, "30"))
    }
    println(getDTFormat()+": REFRESH TABLE ")
    spark.sql(s"refresh table ${sDBName}.csig_logs")
    spark.sql(s"refresh table ${sDBName}.csig_request")
    spark.sql(s"refresh table ${sDBName}.csig_request_merchant")
    spark.sql(s"refresh table ${sDBName}.csig_request_audience")
    spark.sql(s"refresh table ${sDBName}.csig_audience")
    spark.sql(s"refresh table ${sDBName}.csig_response_data")
    //}catch{
    //  case ex: Exception => { println("---Exception while processing: " + ex) }
    //case _: Exception => { println("---Exception while processing--") }
    //System.exit(999)
    //}
    println(getDTFormat() + ": Completed. Exiting spark job.");
  }


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
