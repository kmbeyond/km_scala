import scala.collection.mutable.{Map,SynchronizedMap, HashMap}
/**
  * Created by kiran on 2/20/17.
  */
object ScalaHashMap {

  def main(args: Array[String]) {

    var hm = new HashMap[String, Int]

    hm.put("hello", 1)

    hm += ("hello2" -> 1)


    //add 1 to value of a key
    hm("hello") = hm("hello")+1

  }

}
