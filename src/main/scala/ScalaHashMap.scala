import scala.collection.mutable.{Map,SynchronizedMap, HashMap}
/**
  * Created by kiran on 2/20/17.
  */
object ScalaHashMap {

  def main(args: Array[String]) {

    var hm = new HashMap[String, Int]

    hm.put("hello", 1)

    hm += ("hello2" -> 1)

    println("Printing HashMap: hello=" + hm("hello"));
    println("                  hello2=" + hm.get("hello2").get);
    //add 1 to value of a key
    hm("hello") = hm("hello")+1;
    println("incremented:      hello=" + hm.get("hello").get);

    //Above code throws exception if there is no matching key; so use getOrElse()
    println("Non-existing key: hello3=" + hm.get("hello3").getOrElse("No Match"));

  }

}
