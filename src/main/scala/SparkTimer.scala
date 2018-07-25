package com.km

import java.text.SimpleDateFormat
import java.util.Date
import java.util.{Timer, TimerTask}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkTimer {
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    logger.info(getDT() + "; Class name=" + this.getClass.getName )

    val conf = new SparkConf()
      .setMaster("local") //yarn
      .setAppName("kmiry_Timed_Job")

    val spark = SparkSession.builder
      .config(conf)
      //.enableHiveSupport()
      .getOrCreate

    var intSeconds = 1
    var iNumTimes = 30
    var timeStartMS = System.currentTimeMillis()
    var timeNowMS = System.currentTimeMillis()


    def printMessage() = {

      /*
      timeNowMS = System.currentTimeMillis()
      println(getDT()+": executing.. Time: "+timeStartMS+"; - "+timeNowMS)
      if ( ((timeNowMS - timeStartMS) / 1000) >= intSeconds*60 ) {
        println(getDT()+": exiting now.. ")
        System.exit(0)
      }*/

      println(getDT()+":"+System.currentTimeMillis()+": executing.. #"+ iNumTimes)
      iNumTimes = iNumTimes - 1
      if(iNumTimes <= 0){
        println(getDT()+": exiting now.. ")
        System.exit(0)
      }
    }

    val timer = new Timer()
    timer.schedule(function2TimerTask(printMessage), 0, intSeconds*1000)
    Thread.sleep(50000)
    timer.cancel()

  }

  implicit def function2TimerTask(f: () => Unit): TimerTask = {
    return new TimerTask {
      def run() = f()
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
