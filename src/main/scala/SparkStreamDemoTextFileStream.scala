/**
  * Created by kiran on 2/1/17.
  *
  * STEPS:
  * 1. Start/run the program
  * 2. Create a new file with few words & move to the stream location
  *     NOTE: WORKS ONLY WITH "BRAND" NEW FILES CREATED
  *           File copy & move also does NOT work
  * 3.Code can be changed to take input arguments (args)
  */

//import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3


object SparkStreamDemoTextFileStream {

  def main(args: Array[String]){


    //val ssc = new StreamingContext(args(0), "Streaming from file: wordcount", Seconds(20))
    val ssc = new StreamingContext("local[2]", "Streaming from file: wordcount", Seconds(5))
    ssc.sparkContext.setLogLevel("ERROR")
    //val lines = ssc.socketTextStream(args(1), args(2).toInt)
    //val lines = ssc.socketTextStream("localhost", 50055)
    val lines = ssc.textFileStream("/home/kiran/km/km_big_data/data_realtime/")

    var iWordCounter = 0
    lines.foreachRDD {
      rdd => {
        iWordCounter = iWordCounter +1
        rdd.map(x => ((iWordCounter), x)).foreach(println)
      }
    }
    //(12,how are you? Try for a project.)
    //(20,are you there?)

    //Operation#2: Wordcount
    val wordCounts = lines.flatMap(_.split(" ")).
      map(x => (x,1)).
      reduceByKey(_+_)

    print("File Data for each stream (word, 1) is printing..")
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}