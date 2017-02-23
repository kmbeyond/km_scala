/**
  * Created by kiran on 2/3/17.
  */

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.Duration

object StreamDemoJavaStreamCtx {
  def main(args: Array[String]): Unit = {


    //val ctx = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
    //val ssc = new JavaStreamingContext(ctx, new Duration(10000));

    //JavaStreamingContext jsc = new JavaStreamingContext("local[2]","Java Spark Streaming", new Duration(10))
    //val lines = jsc.socketTextStream("localhost", 50050)


  }
}


/*

    import org.apache.spark.streaming._
    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

    //val ssc = new StreamingContext(args(0), "wordcount", Seconds(20))
    val ssc = new StreamingContext("local[2]", "Streaming from file: wordcount", Seconds(20))

    //val lines = ssc.textFileStream(args(1))
    //val lines = ssc.textFileStream("/home/kiran/km/km_hadoop/spark/")
    val lines = ssc.socketTextStream("localhost", 50055)

    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(x => (x,1))
    val wordCounts = pairs.reduceByKey(_+_)

    print("File Data (word, 1) is printing..")
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

 */