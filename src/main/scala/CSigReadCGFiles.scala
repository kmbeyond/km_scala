package com.wp.da.csig

import java.text.SimpleDateFormat
import java.util.Date
//import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CSigReadCGFiles {
  //val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    //logger.info(getDT() + "; Class name=" + this.getClass.getName )
    println(getDTFormat() + ": ********** CSigReadCGFiles **********");
    //try {
    //input arguments
    if (args.size < 2) {
      println("Please verify input arguments:");
      println("0: Database name (Ex: dev_data_comm)")
      println("1: HDFS Path for audience files")
      System.exit(101)
    }

    val sDBName = args(0);
    println(getDTFormat() + ": ***** arg(0): " + sDBName);
    val inputHDFSPath = args(1);
    println(getDTFormat() + ": ***** arg(1): " + inputHDFSPath);

    //sDBName="dev_data_comm"
    //inputHDFSPath="/lake/transform/worldpay/data_comm/csig"

    val spark = SparkSession.builder
      .appName("CSig Control Group data load")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    import org.apache.spark.sql.functions._
    import sys.process._
    import spark.implicits._

    //val proc_st_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    val proc_dt = new java.text.SimpleDateFormat("yyyy-MM-dd").format(new java.util.Date())
    var run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts', 'CSIG_CG', '002', 'START', 0, 'CG input files load Process Started'")

    println(getDT() + s":------ Reading JSON: ${inputHDFSPath} --------")
    val csigReq = spark.read.json(inputHDFSPath + "/incoming/control_group/*.jsn");
    //csigReq.show(false)

    var csigReqColumns = csigReq.columns
    if (csigReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ":-------------- Data is invalid ------------------")
      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts', 'CSIG_CG', '002_01', 'csig_cg_request', 1, 'Corrupt data file'")
      System.exit(102)
    }

    //-------------001: .csig_cg_request--------------
    val df_csig_cg_request = csigReq.
      withColumn("mpStartDate", $"mediaPeriod.startDate").
      withColumn("mpEndDate", $"mediaPeriod.endDate").
      withColumn("tpStartDate", $"testPeriod.startDate").
      withColumn("tpEndDate", $"testPeriod.endDate").
      withColumn("merchants", explode($"merchants")).
      //withColumn("request_date", lit(java.time.LocalDate.now.toString)).
      select($"id" //,lit(proc_st_ts).as("process_ts"), lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts"),
        , $"mpStartDate", $"mpEndDate", $"tpStartDate", $"tpEndDate" ,$"merchants"
        , lit(proc_dt).as("process_date"))

    csigReqColumns = df_csig_cg_request.columns
    if (csigReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ":-------------- json data is invalid: df_csig_cg_request ------------------")
      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts', 'CSIG_CG', '002_01', 'csig_cg_request', 1, 'Corrupt JSON data file'")
      System.exit(102)
    }
    println(getDTFormat() + s": --> ${sDBName}.csig_cg_request")
    df_csig_cg_request.coalesce(1).write.mode("overwrite").insertInto(sDBName+".csig_cg_request")
    //df_csig_cg_request.coalesce(1).write.partitionBy("process_date").saveAsTable(sDBName+".csig_cg_request")
    //nodf_csig_request.createOrReplaceTempView("df_csig_cg_request")
    //nospark.sql(s"insert into $sDBName.df_csig_cg_request select id,'$proc_st_ts','$run_time_ts',mpStartDate,mpEndDate,tpStartDate,tpEndDate,merchants,aud_id,aud_uri,aud_count,process_date FROM df_csig_request")
    println(getDTFormat() + s":      Inserted.");
    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts','CSIG_CG','002_01','csig_cg_request', 0, 'Inserted CG Requests.'")

    spark.sql(s"REFRESH TABLE $sDBName.csig_cg_request");

    //-------------003: csig_cg_request_audience--------------
    val df_csig_cg_req_aud_exp = csigReq.select($"id".as("id"), explode($"audiences").as("audiences"))

    csigReqColumns = df_csig_cg_req_aud_exp.columns
    if (csigReqColumns.contains("_corrupt_record")) {
      println(getDTFormat() + ":-------------- json data is invalid: csig_cg_request_audience ------------------")
      run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
      spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts','CSIG_CG','002_03','csig_cg_request_audience',1,'Corrupt JSON data file'")
      System.exit(102)
    }
    //val df_csig_cg_request_audience = df_csig_cg_req_aud_exp.select($"request_id", lit(proc_st_ts).as("process_ts"),lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts"), $"audience_id", $"uri", $"source", $"count")
    val df_csig_cg_request_audience = df_csig_cg_req_aud_exp.select($"id",
      //lit(proc_st_ts).as("process_ts"), lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts"),
      $"audiences.id".as("audience_id"), $"audiences.uri", $"audiences.count"
      , lit(proc_dt).as("process_date") )

    println(getDTFormat() + s": --> ${sDBName}.csig_cg_request_audience")
    df_csig_cg_request_audience.coalesce(1).write.mode("overwrite").insertInto(sDBName+".csig_cg_request_audience")
    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    //nodf_csig_cg_request_audience.createOrReplaceTempView("df_csig_cg_request_audience")
    //nospark.sql(s"insert into $sDBName.csig_cg_request_audience select request_id,'$proc_st_ts','$run_time_ts',audiences FROM df_csig_cg_request_audience")
    println(getDTFormat() + s":              Inserted in ${sDBName}.csig_cg_request_audience");
    spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts','CSIG_CG','002_03','csig_cg_request_audience',0,'Inserted CG Request Audiences'")
    spark.sql(s"REFRESH TABLE $sDBName.csig_cg_request_audience")

    //-------------004: csig_cg_audience--------------
    def funFileName: ((String) => String) = { (s) =>(s.split("/").last)}
    import org.apache.spark.sql.functions.udf
    val udfFileName = udf(funFileName)

    //var df_cg_aud=spark.read.csv(inputHDFSPath+ "/incoming/control_group/*.gz").toDF("cs_sha").withColumn("uri", udfFileName(input_file_name()))
    var df_cg_aud=spark.read.csv(inputHDFSPath+ "/incoming/control_group/*.gz").toDF("cs_sha").withColumn("uri", udfFileName(input_file_name())).
      join(df_csig_cg_request_audience, Seq("uri"), "left_outer")
    df_cg_aud.cache()
    val df_audience=df_cg_aud.   // df_csig_cg_request_audience.select("uri","id","audience_id").join(df_cg_aud, Seq("uri")).
      select($"id"
        //, lit(proc_st_ts).as("process_ts")  , lit(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())).as("create_ts")
        , $"audience_id", $"cs_sha"
        , lit(proc_dt).as("process_date")
      )
    println(getDTFormat() + s": --> ${sDBName}.csig_cg_audience")
    df_audience.coalesce(4).write.mode("overwrite").insertInto(sDBName+".csig_cg_audience");
    //df_audience.createOrReplaceTempView("df_audience");
    //spark.sql(s"INSERT into $sDBName.csig_cg_audience partition (process_date='" + (java.time.LocalDate.now) + s"') SELECT * from df_audience");
    println(getDTFormat() + s":               Inserted in ${sDBName}.csig_cg_audience");
    spark.sql(s"REFRESH TABLE $sDBName.csig_cg_audience")

    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"INSERT INTO TABLE $sDBName.csig_logs SELECT '$run_time_ts','CSIG_CG','002_04','csig_cg_audience',0,'Inserted CG Audience data'")

    //--- Create CG Aud summary file - START
    var sCurrDt = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new java.util.Date())
    df_csig_cg_request_audience.select("id","uri").join(df_cg_aud, Seq("id","uri"), "left_outer").filter($"audience_id".isNull).groupBy("id","uri").count.withColumn("count", lit("0")).
      coalesce(1).write.options(Map("header"->"True")).csv(s"${inputHDFSPath}/outgoing/control_group/${sCurrDt}")
    s"hdfs dfs -mv ${inputHDFSPath}/outgoing/control_group/${sCurrDt}/*.csv ${inputHDFSPath}/outgoing/control_group/CSIG_cg_aud_data_miss_${sCurrDt}.csv".!
    println(getDTFormat() + s": --> Summary file (Missed Audience files): ${inputHDFSPath}/outgoing/control_group/CSIG_cg_aud_data_miss_${sCurrDt}.csv")
    s"hdfs dfs -rm -R $inputHDFSPath/outgoing/control_group/$sCurrDt".!

    sCurrDt = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new java.util.Date())
    df_csig_cg_request_audience.select("id","uri").join(df_cg_aud, Seq("id","uri"), "left_outer").filter($"audience_id".isNotNull).groupBy("id","uri").count.
      coalesce(1).write.options(Map("header"->"True")).csv(s"${inputHDFSPath}/outgoing/control_group/${sCurrDt}")
    s"hdfs dfs -mv ${inputHDFSPath}/outgoing/control_group/${sCurrDt}/*.csv ${inputHDFSPath}/outgoing/control_group/CSIG_cg_aud_summary_${sCurrDt}.csv".!
    println(getDTFormat() + s": --> Summary file (Audience files): ${inputHDFSPath}/outgoing/control_group/CSIG_cg_aud_summary_${sCurrDt}.csv")
    s"hdfs dfs -rm -R $inputHDFSPath/outgoing/control_group/$sCurrDt".!

    //--- Create CG Aud summary file - END


    //if (new scala.util.Random().nextInt(10) % 3 == 0) {
    com.wp.da.csig.CSigCleanup.main(Array(sDBName, inputHDFSPath, "30"))
    //}

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
