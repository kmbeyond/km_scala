/**
  * Created by kiran on 2/6/17.
  */
object SparkStreamDemoCustTxnsJoin {

  def main(args: Array[String]){
    import org.apache.spark._
    import org.apache.spark.streaming._
    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

    def updateFunction(rows: Seq[Double] , runningVal: Option[Double]) = {
      val newVal = rows.sum + runningVal.getOrElse(0.0)
      new Some(newVal)
    }
    val ssc = new StreamingContext("local[2]", "Streaming Txns files & join with Cust RDD from file", Seconds(2))
    val strData = ssc.socketTextStream("localhost", 50055)

    ssc.checkpoint("/home/kiran/km/km_hadoop_op/op_spark/op_streaming")

    //Read customer data from file/HDFS
    //input data is in format:
    //custId|custname|....
    val customerDataRDD = ssc.sparkContext.textFile("/home/kiran/km/km_hadoop/data/data_customers").map( row => {
      val custValues = row.split('|')
      (custValues(0), custValues(1))
    })

    //read stream data of txns
    //input data is data of format:
    //txnId, custId, itemId, itemValue
    val cartStream = strData.map( row => {
      val txnValues = row.split(",")
      //val custId = txnValues(1)
      (txnValues(1), txnValues(3))
      //(txnValues(1), row) //if need to return whole row
    })

    val joinRDD = cartStream.transform( inRDD => {
      customerDataRDD.join(inRDD).map {
        case(customerId, (customerName, sale)) => {
          (customerName, sale)
        }
      }
    })

    val perCustSalesStream = joinRDD.map{
      case(customerName,sale) => {
        val salesAmount = sale.toDouble
        //val salesAmount = sale.split(",")(3).toDouble //used if the whole sale record is mapped during stream
        (customerName, salesAmount)
      }
    }

    val perCustomerSales = perCustSalesStream.updateStateByKey[Double](updateFunction _)
    perCustomerSales.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
