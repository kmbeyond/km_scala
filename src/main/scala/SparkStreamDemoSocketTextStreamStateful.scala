/**
  * Created by kiran on 2/1/17.
  *
  * Input args:
  * 0: num of threads ( local[2] )
  * 1: Cluster manager ( localhost)
  * 2: port number ( 50055 )
  */
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming._ // not necessary since Spark 1.3


object SparkStreamDemoSocketTextStreamStateful {

  def main(args: Array[String]){


    def updateFunction(values: Seq[Int], runningCount: Option[Int]) = {
      val newCount = values.sum + runningCount.getOrElse(0)
      new Some(newCount)
    }

    val ssc = new StreamingContext("local[2]", "Streaming from file: wordcount", Seconds(20))

    val lines = ssc.socketTextStream("localhost", 50055)
    ssc.checkpoint("/home/kiran/km/km_hadoop_op/op_spark/op_streaming")

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(x => (x,1))
    val wordCounts = pairs.reduceByKey(_+_)
    val totalWordCount = wordCounts.updateStateByKey(updateFunction _)

    print("Stateful Socket Streaming (word, 1) is printing..")
    totalWordCount.print()
    ssc.start()
    ssc.awaitTermination()

  }

}