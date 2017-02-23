/**
  * Created by kiran on 2/1/17.
  */
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3


object StreamDemoFile {

  def main(args: Array[String]){


    //val ssc = new StreamingContext(args(0), "Streaming from file: wordcount", Seconds(20))
    val ssc = new StreamingContext("local[2]", "Streaming from file: wordcount", Seconds(20))

    //val lines = ssc.textFileStream(args(1))
    val lines = ssc.textFileStream("/home/kiran/km/km_hadoop_op/op_spark/op_streaming")
    //val lines = ssc.socketTextStream("localhost", 50055)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(x => (x,1))
    val wordCounts = pairs.reduceByKey(_+_)

    print("File Data (word, 1) is printing..")
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}