package com.wp.da.cs

import java.text.SimpleDateFormat
import java.util.Date
//import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object json_csProcess01 {
  //val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    //logger.info(getDT() + "; Class name=" + this.getClass.getName )

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

    //val conf = new SparkConf()
    //.setMaster("yarn") //yarn
    //.setAppName("KM CS")

    val spark = SparkSession.builder
      .appName("Commerce Signals")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()
    //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    //.config("spark.hadoop.validateOutputSpecs", "false")
    //.config("spark.debug.maxToStringFields", 100)
    spark.sparkContext.setLogLevel("ERROR")

    import org.apache.spark.sql.functions._
    //import spark.implicits._
    import sys.process._

    import spark.implicits._

    val proc_st_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())

    var run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"insert into table $sDBName.cs_logs select '$run_time_ts', 'cs', '002', 'START', 0, 'JSON load Process Started'")

    println(getDT() + s":------ Reading JSON: ${inputHDFSPath} --------")
    val csReq = spark.read.json(inputHDFSPath + "/incoming/*.jsn") // + inputJsonFile);
    //csReq.show(false)

    var csReqColumns = csReq.columns
    if (csReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ":-------------- json data is invalid ------------------")
      System.exit(102)
    }

    //-------------001: .cs_request--------------
    val df_cs_request = csReq.withColumn("dateRanges", explode($"dateRanges")).
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

    csReqColumns = df_cs_request.columns
    if (csReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ":-------------- json data is invalid: df_cs_request ------------------")
      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table $sDBName.cs_logs select '$run_time_ts', 'cs', '002_01', 'cs_request', 1, 'Corrupt JSON data file'")
      System.exit(102)
    }
    println(getDTFormat() + s": --> ${sDBName}.cs_request")
    df_cs_request.coalesce(1).write.insertInto(sDBName+".cs_request")
    //nodf_cs_request.createOrReplaceTempView("df_cs_request")
    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    //nospark.sql(s"insert into $sDBName.cs_request select request_id,'$proc_st_ts','$run_time_ts',description,start_date,end_date,time_zone,request_date,transaction_amount,transaction_count,matched_ids_count,transactor_count,stddev_spend_txn,stddev_spend_transactor,stddev_txncnt_transactor FROM df_cs_request")
    println(getDTFormat() + s": Inserted in ${sDBName}.cs_request");
    spark.sql(s"insert into table $sDBName.cs_logs select '$run_time_ts','cs','002_01','cs_request', 0, 'Inserted requests: '")

    //-------------002: cs_request_merchant--------------
    val df_cs_request_merchant = csReq.select($"requestid".as("request_id"),lit(proc_st_ts).as("process_ts"),lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts"), concat_ws(",", $"merchants").as("merchants"), explode($"merchants").as("merchant_id"))
    csReqColumns = df_cs_request_merchant.columns
    if (csReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ": -------------- json data is invalid: df_cs_request_merchant ------------------");
      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table $sDBName.cs_logs select '$run_time_ts', 'cs', '002_02', 'cs_request_merchant', 1, 'Corrupt JSON data file'")
      System.exit(102)
    }
    println(getDTFormat() + s": --> ${sDBName}.cs_request_merchant")
    df_cs_request_merchant.coalesce(1).write.insertInto(sDBName+".cs_request_merchant")
    //nodf_cs_request_merchant.createOrReplaceTempView("df_cs_request_merchant")
    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    //nospark.sql(s"INSERT into $sDBName.cs_request_merchant SELECT request_id,'$proc_st_ts','$run_time_ts',merchants,merchant_id FROM df_cs_request_merchant")
    println(getDTFormat() + s": Inserted in ${sDBName}.cs_request_merchant");
    spark.sql(s"insert into table $sDBName.cs_logs select '$run_time_ts','cs','002_02','cs_request_merchant',0,'Inserted Request Merchants'")

    //-------------003: cs_request_audience--------------
    val df_cs_request_audience_exp = csReq.select($"requestid".as("request_id"), explode($"audiences").as("audiences")).
      withColumn("request_id", $"request_id").
      withColumn("audience_id", $"audiences.id").
      withColumn("uri", $"audiences.uri").
      withColumn("source", $"audiences.source").
      withColumn("count", $"audiences.count")

    csReqColumns = df_cs_request_audience_exp.columns
    if (csReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ":-------------- json data is invalid: cs_request_audience ------------------")
      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table $sDBName.cs_logs select '$run_time_ts','cs','002_03','cs_request_audience',1,'Corrupt JSON data file'")
      System.exit(102)
    }
    val df_cs_request_audience = df_cs_request_audience_exp.select($"request_id", lit(proc_st_ts).as("process_ts"),lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts"), $"audience_id", $"uri", $"source", $"count")
    println(getDTFormat() + s": --> ${sDBName}.cs_request_audience")
    df_cs_request_audience.coalesce(1).write.insertInto(sDBName+".cs_request_audience")
    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    //nodf_cs_request_audience.createOrReplaceTempView("df_cs_request_audience")
    //nospark.sql(s"insert into $sDBName.cs_request_audience select request_id,'$proc_st_ts','$run_time_ts',audiences FROM df_cs_request_audience")
    println(getDTFormat() + s": Inserted in ${sDBName}.cs_request_audience");
    spark.sql(s"insert into table $sDBName.cs_logs select '$run_time_ts','cs','002_03','cs_request_audience',0,'Inserted Request Audiences'")

    //-------------004: cs_audience--------------
    /*
        val req_aud_km = df_cs_request_audience.map(rec => (rec.getString(4), (rec.getString(0), rec.getString(3)))).rdd.groupByKey.mapValues(_.toList).collectAsMap
        for ((fileName, fileInfo) <- req_aud_km)
        {
          println(getDTFormat() + ":--- Audience File: " + fileName);
          val df_audience_file = spark.read.csv(inputHDFSPath + "/incoming/" + fileName)

    val df_audience=Seq(("","","","","","")).toDF("cs_sha","request_id","process_ts","create_ts","audience_id","impression_window")
          if(df_audience_file.columns.size > 0 ) {
            val df_audience = df_audience_file.toDF("cs_sha").select(lit(fileInfo(0)._1).as("request_id")
              , lit(proc_st_ts).as("process_ts")
              , lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts")
              , lit(fileInfo(0)._2).as("audience_id"), $"cs_sha", lit("").as("impression_window")
              //,lit(java.time.LocalDate.now.toString).as("process_date")
            );

            //nodf_audience.repartition(1).write.insertInto(sDBName+".cs_audience")
            df_audience.createOrReplaceTempView("df_audience");
            println(getDTFormat() + s": --> ${sDBName}.cs_audience")
            spark.sql(s"INSERT into $sDBName.cs_audience partition (process_date='" + (java.time.LocalDate.now) + s"') SELECT * from df_audience");
            run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
            println(getDTFormat() + s": Inserted ${fileName} in ${sDBName}.cs_audience");
            //spark.sql(s"INSERT INTO TABLE $sDBName.cs_logs SELECT '$run_time_ts','cs','002_04','cs_audience',0,'Inserted Audience data file: $fileName; RowCount:" + df_audience.count + "'")
          }
        }
    */
    def funFileName: ((String) => String) = { (s) =>(s.split("/").last)}
    import org.apache.spark.sql.functions.udf
    val udfFileName = udf(funFileName)

    var df_audience_data=spark.read.csv(inputHDFSPath+ "/incoming/*.gz").toDF("cs_sha").withColumn("uri", udfFileName(input_file_name()))

    val df_audience=df_cs_request_audience.select("uri","request_id","audience_id").
      join(df_audience_data, Seq("uri")).
      select($"request_id"
        , lit(proc_st_ts).as("process_ts")
        , lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts")
        , $"audience_id", $"cs_sha", lit("").as("impression_window")
      )
    df_audience.createOrReplaceTempView("df_audience");
    println(getDTFormat() + s": --> ${sDBName}.cs_audience")
    spark.sql(s"INSERT into $sDBName.cs_audience partition (process_date='" + (java.time.LocalDate.now) + s"') SELECT * from df_audience");
    println(getDTFormat() + s": Inserted in ${sDBName}.cs_audience");

    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"INSERT INTO TABLE $sDBName.cs_logs SELECT '$run_time_ts','cs','002_04','cs_audience',0,'Inserted Audience data'")

    //--- Create Aud summary file - START
    val df_audience_files=df_cs_request_audience.select("uri","request_id","audience_id").
      join(df_audience_data, Seq("uri"), "left_outer").
      select($"request_id",$"cs_sha", $"uri"  )

    import sys.process._
    var sCurrDt = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new java.util.Date())
    df_audience_files.groupBy("request_id","uri").agg(sum(when($"cs_sha".isNull,0).otherwise(1)).alias("aud_count")).filter($"aud_count" === 0).
      coalesce(1).write.csv(s"${inputHDFSPath}/outgoing/${sCurrDt}")
    s"hdfs dfs -mv ${inputHDFSPath}/outgoing/${sCurrDt}/*.csv ${inputHDFSPath}/outgoing/cs_aud_data_miss_${sCurrDt}.csv".!
    println(getDTFormat() + s": --> Audience Summary file: ${inputHDFSPath}/outgoing/cs_aud_summary_${sCurrDt}.csv")
    s"hdfs dfs -rm -R $inputHDFSPath/outgoing/$sCurrDt".!

    sCurrDt = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new java.util.Date())

    df_audience_files.groupBy("request_id","uri").agg(sum(when($"cs_sha".isNull,0).otherwise(1)).alias("aud_count")).
      coalesce(1).write.csv(s"${inputHDFSPath}/outgoing/${sCurrDt}")
    s"hdfs dfs -mv ${inputHDFSPath}/outgoing/${sCurrDt}/*.csv ${inputHDFSPath}/outgoing/cs_aud_summary_${sCurrDt}.csv".!
    println(getDTFormat() + s": --> Audience Summary file: ${inputHDFSPath}/outgoing/cs_aud_summary_${sCurrDt}.csv")
    s"hdfs dfs -rm -R $inputHDFSPath/outgoing/$sCurrDt".!

    //--- Create Aud summary file - END

    //------------Populate output data
    //Moved to beeline command

    //-------------006: cs_response_data--------------
    //Moved to new class file

    if (new scala.util.Random().nextInt(10) % 3 == 0) {
      com.wp.da.cs.csCleanup.main(Array(sDBName, inputHDFSPath, "30"))
    }
    println(getDTFormat()+": REFRESH TABLE ")
    spark.sql(s"refresh table ${sDBName}.cs_logs")
    spark.sql(s"refresh table ${sDBName}.cs_request")
    spark.sql(s"refresh table ${sDBName}.cs_request_merchant")
    spark.sql(s"refresh table ${sDBName}.cs_request_audience")
    spark.sql(s"refresh table ${sDBName}.cs_audience")
    spark.sql(s"refresh table ${sDBName}.cs_response_data")
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
