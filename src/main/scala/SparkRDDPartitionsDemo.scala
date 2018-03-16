import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession

/**
  * Created by kiran on 2/20/17.
  */
object SparkRDDPartitionsDemo {


  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQLContext Read CSV Demo")
      .getOrCreate

    spark.conf.set("spark.executor.memory", "2g")
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val a = sc.parallelize(1 to 99999, 10)

    println("Print data....")
    println("Map....")
    a.map(x => (x,1)).take(10).foreach(println)

    println("Partition-Map....")
    a.foreachPartition( prtn => {
      prtn.map(x => (x,1)).take(10).foreach(println)
    })

    println("Get total of all...")
    println("Using Map....start: "+getDT())
    val timeStart = System.nanoTime()
    a.map(x => (1,x)).reduceByKey(_+_).foreach(println)
    val timeEnd = System.nanoTime()
    println("Using Map....end. Time diff: "+ ( timeEnd-timeStart) +" ns")
    println("Using Map....end. Time diff: "+ ( timeEnd-timeStart)/1000 +" micro sec")
    println("Using Map....end. Time diff: "+ ( timeEnd-timeStart)/1e3 +" micro sec")

    println("Using MapPartitions....")
    def addMap(numbers: Iterator[Int]) : Iterator[Int] = {
       var sum = 0
       while (numbers.hasNext) {
           sum = sum + numbers.next()
         }
       return Iterator(sum)
     }

     a.mapPartitions(addMap).map(x => (1,x)).reduceByKey(_+_).foreach(println)



  }
  def getDT(): String ={

    val dateFormatter = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss")
    return dateFormatter.format(new Date())

    //val today = Calendar.getInstance.getTime
    //return dateFormatter.format(today)

  }
}