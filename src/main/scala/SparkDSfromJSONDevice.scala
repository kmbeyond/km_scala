import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.when

/**
  * Created by kiran on 2/20/17.
  */
object SparkDSfromJSONDevice {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
      .config("spark.executor.memory", "2g")
      .config("spark.yarn.executor.memoryOverhead", "10g")
      .master("local")
      .appName("Spark DF")
      .getOrCreate

    //spark.conf.set("spark.executor.memory", "2g")

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val sJSONPath = "file:///C://km//avro//device_data.json"
    //Sample data:
    //{"performCheck" : "N", "clientTag" :{"key":"111"}, "contactPoint": {"email":"abc@gmail.com", "type":"EML"}}
    //{"performCheck" : "N", "clientTag" :{"key":"222"}, "contactPoint": {"email":"def@gmail.com", "type":"EML"}}

    import org.apache.spark.sql.types._                         // include the Spark Types to define our schema
    import org.apache.spark.sql.functions._                     // include the Spark helper functions

    val jsonSchema = new StructType()
      .add("battery_level", LongType)
      .add("c02_level", LongType)
      .add("cca3",StringType)
      .add("cn", StringType)
      .add("device_id", LongType)
      .add("device_type", StringType)
      .add("signal", LongType)
      .add("ip", StringType)
      .add("temp", LongType)
      .add("timestamp", TimestampType)
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    //jsonSchema: org.apache.spark.sql.types.StructType = StructType(StructField(battery_level,LongType,true), StructField(c02_level,LongType,true), StructField(cca3,StringType,true), StructField(cn,StringType,true), StructField(device_id,LongType,true), StructField(device_type,StringType,true), StructField(signal,LongType,true), StructField(ip,StringType,true), StructField(temp,LongType,true), StructField(timestamp,TimestampType,true))

    // define a case class
    case class DeviceData (id: Int, device: String)

    val ds = spark.read.json(sJSONPath)
    ds.printSchema()

    ds.show(false)



  }
}

/*
data
file:///C://km//avro//device_data.json
{"device_id": 0, "device_type": "sensor-ipad", "ip": "68.161.225.1", "cca3": "USA", "cn": "United States", "temp": 25, "signal": 23, "battery_level": 8, "c02_level": 917, "timestamp" :1475600496 }
{"device_id": 1, "device_type": "sensor-igauge", "ip": "213.161.254.1", "cca3": "NOR", "cn": "Norway", "temp": 30, "signal": 18, "battery_level": 6, "c02_level": 1413, "timestamp" :1475600498 }
{"device_id": 2, "device_type": "sensor-ipad", "ip": "88.36.5.1", "cca3": "ITA", "cn": "Italy", "temp": 18, "signal": 25, "battery_level": 5, "c02_level": 1372, "timestamp" :1475600500 }
{"device_id": 3, "device_type": "sensor-inest", "ip": "66.39.173.154", "cca3": "USA", "cn": "United States", "temp": 47, "signal": 12, "battery_level": 1, "c02_level": 1447, "timestamp" :1475600502 }
{"device_id": 4, "device_type": "sensor-ipad", "ip": "203.82.41.9", "cca3": "PHL", "cn": "Philippines", "temp": 29, "signal": 11, "battery_level": 0, "c02_level": 983, "timestamp" :1475600504 }
{"device_id": 5, "device_type": "sensor-istick", "ip": "204.116.105.67", "cca3": "USA", "cn": "United States", "temp": 50, "signal": 16, "battery_level": 8, "c02_level": 1574, "timestamp" :1475600506 }
{"device_id": 6, "device_type": "sensor-ipad", "ip": "220.173.179.1", "cca3": "CHN", "cn": "China", "temp": 21, "signal": 18, "battery_level": 9, "c02_level": 1249, "timestamp" :1475600508 }
{"device_id": 7, "device_type": "sensor-ipad", "ip": "118.23.68.227", "cca3": "JPN", "cn": "Japan", "temp": 27, "signal": 15, "battery_level": 0, "c02_level": 1531, "timestamp" :1475600512 }
{"device_id": 8, "device_type": "sensor-inest", "ip": "208.109.163.218", "cca3": "USA", "cn": "United States", "temp": 40, "signal": 16, "battery_level": 9, "c02_level": 1208, "timestamp" :1475600514 }
{"device_id": 9, "device_type": "sensor-ipad", "ip": "88.213.191.34", "cca3": "ITA", "cn": "Italy", "temp": 19, "signal": 11, "battery_level": 0, "c02_level": 1171, "timestamp" :1475600516 }
{"device_id": 10, "device_type": "sensor-igauge", "ip": "68.28.91.22", "cca3": "USA", "cn": "United States", "temp": 32, "signal": 26, "battery_level": 7, "c02_level": 886, "timestamp" :1475600518 }

 */
