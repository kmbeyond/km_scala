package com.wp.da.csig

import java.text.SimpleDateFormat
import java.util.Date
//import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CSigGenerateResponse {
  //val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    //logger.info(getDTFormat() + "; Class name=" + this.getClass.getName )
    println(getDTFormat() + ": ********** CSigGenerateResponse **********");
    //try {
      //input arguments
      if (args.size < 2) {
        println("Please verify input arguments:");
        println("0: Database name (Ex: dev_data_comm)")
        println("1: HDFS Path for response file")
        System.exit(101)
      }

    val sDBName = args(0);
    println(getDTFormat() + ": ***** arg(0): " + sDBName);
      val opHDFSPath = args(1);
      println(getDTFormat() + ": ***** arg(1): " + opHDFSPath);

    val spark = SparkSession.builder()
      .appName("Commerce Signals Generate Response")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR");

    import org.apache.spark.sql.functions._
    import spark.implicits._
    import sys.process._

    val proc_st_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    var run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"insert into table $sDBName.csig_logs select '$run_time_ts', 'CSIG', '004', 'Response Spark Job', 0, 'Response Process Started'")


      //-------------006: csig_response_data--------------
      //val df_csig_response_data = spark.sql(s"SELECT * FROM $sDBName.csig_response_data WHERE request_id='$sRequestId' AND create_ts = (SELECT max(create_ts) FROM $sDBName.csig_response_data WHERE request_id='$sRequestId')") //.show(false)
      val df_csig_response_data = spark.sql(s"SELECT * FROM $sDBName.csig_response_data_stg")

      //if( json_csig_resp.select("results_req").collect.size >= 1) {
      if (df_csig_response_data.select("request_id").collect.size >= 1) {

        val json_csig_resp = df_csig_response_data.withColumn("statusCode", lit("SUCCESS")).withColumn("stdDevType", lit("SAMPLE")).
          groupBy("request_id", "description", "start_date", "end_date", "audience_id", "card_present_ind", "statusCode","stdDevType").
          agg(max("merchant_id").as("merchants") //collect_set("merchant_id").as("merchants")
            , max("statusCode")
            ,round(max("transaction_amount"),2).as("transactionAmount")
            ,round(max("transaction_count")).as("transactionCount")
            ,round(max("matched_ids_count")).as("matchedIdsCount")
            ,round(max("transactor_count")).as("transactorCount")
            ,round(max("stddev_spend_transactor"),4).as("stdDevSpendByTransactor")
            ,round(max("stddev_txncnt_transactor"),4).as("stdDevTxnCntByTransactor")
            ,round(max("stddev_spend_txn"),4).as("stdDevSpendByTxn"), max("stdDevType")
          ).
          withColumn("results_ch", (struct($"card_present_ind".as("channel"), $"statusCode"
            ,$"transactionAmount",$"transactionCount",$"matchedIdsCount",$"transactorCount"
            ,$"stdDevSpendByTransactor",$"stdDevTxnCntByTransactor",$"stdDevSpendByTxn", $"stdDevType" ))
          ). //select($"description",$"audience_id", $"merchants", to_json($"results_ch").as("results_ch")).show(false)
          groupBy("request_id", "description", "start_date", "end_date", "audience_id").
          agg(collect_set("merchants").as("merchants")
            ,sort_array(collect_set("results_ch"), asc=false).as("results_ch")).
          withColumn("results_aud", struct($"merchants", $"audience_id".as("audienceId"), $"results_ch".as("metrics"))
          ). //select($"audience_id", $"merchants", to_json($"results_aud").as("results_aud")).show(1,false)
          groupBy("request_id", "description", "start_date", "end_date").
          agg(collect_set("results_aud").as("results_aud")).
          withColumn("results_req_dt", (struct(
            struct($"description", $"start_date".as("startDate"), $"end_date".as("endDate")).as("dateRange"),
            $"results_aud".as("dateRangeResults")
          ))). //select("results_req_dt").show(false)
          groupBy("request_id").
          agg(sort_array(collect_set("results_req_dt"), asc=false).as("results_req_dt")).
          withColumn("resp", to_json(
            struct($"request_id".as("requestId"), $"results_req_dt".as("results"))
          )).select(concat(lit("CSIG_resp_"), $"request_id", lit("_"+new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new java.util.Date())),lit(".json")).as("filename"), $"resp") //.show(false)

        val resp_km = json_csig_resp.map(rec => (rec.getString(0), rec.getString(1))).rdd.collectAsMap
        //val resp_km = json_csig_resp.rdd.map(r => (r.getString(0), r.getString(1))).collectAsMap()

        val sCurrDt = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new java.util.Date())
        for ((fileName, data) <- resp_km) {
          json_csig_resp.filter($"filename" === fileName).select("resp").coalesce(1).write.text(s"${opHDFSPath}/${sCurrDt}")
          s"hdfs dfs -mv ${opHDFSPath}/${sCurrDt}/*.txt ${opHDFSPath}/${fileName}".!
          println(getDTFormat() + s": --> Outgoing JSON: ${opHDFSPath}/${fileName}")
          s"hdfs dfs -rm -R $opHDFSPath/$sCurrDt".!
        }
        //sCurrDt = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new java.util.Date())
        json_csig_resp.select("filename").coalesce(1).write.text(s"${opHDFSPath}/${sCurrDt}")
        s"hdfs dfs -mv ${opHDFSPath}/${sCurrDt}/*.txt ${opHDFSPath}/CSIG_resp_${sCurrDt}.tag".!
        println(getDTFormat() + s": --> Outgoing tag file: ${opHDFSPath}/CSIG_resp_${sCurrDt}.tag")
        s"hdfs dfs -rm -R $opHDFSPath/$sCurrDt".!

        println(getDTFormat() + s": --> ${sDBName}.csig_response_data")
        spark.sql(s"insert into ${sDBName}.csig_response_data select * from ${sDBName}.csig_response_data_stg")

        run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
        spark.sql(s"insert into table ${sDBName}.csig_logs select '${run_time_ts}','CSIG','004','csig_response_data',0,'${opHDFSPath}/'")
        //spark.sql(s"truncate table ${sDBName}.csig_response_data_stg")

  } else {
    run_time_ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date())
    spark.sql(s"insert into table ${sDBName}.csig_logs select '${run_time_ts}','CSIG','004','csig_response_data',1,'No output data to generate output'")
  }

    spark.sql(s"refresh table ${sDBName}.csig_logs")
    spark.sql(s"refresh table ${sDBName}.csig_response_data")
    spark.sql(s"refresh table ${sDBName}.csig_response_data_stg")

//}catch{
//  case ex: Exception => { println("---Exception while processing: " + ex) }
  //case _: Exception => { println("---Exception while processing--") }
    //System.exit(999)
//}
    println(getDTFormat() + ": Exiting spark job.");
}

def getDT(): String = {

  val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
  return dateFormatter.format(new Date())

  //val today = Calendar.getInstance.getTime
  //return dateFormatter.format(today)
}
  def getDTFormat(): String = {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(new Date())
  }
}
