/**
  * Created by kiran on 2/14/17.
  */
object MapDemo {

  def main(args: Array[String]) {
    val ratings = Map("Lady in the Water" -> 3.0,
      "Snakes on a Plane" -> 4.0,
      "You, Me and Dupree" -> 3.5)

    println("using for..")
    for ((k,v) <- ratings) println(s"key: $k, value: $v")

    println("using foreach..")
    ratings.foreach(x => println(s"key: ${x._1}, value: ${x._2}"))

    println("using foreach-case..")
    ratings.foreach {
      case(movie, rating) => println(s"key: $movie, value: $rating")
    }

    println("getting keys.foreach..")
    ratings.keys.foreach(println)
    //same as
    ratings.keys.foreach((movie) => println(movie))

    println("getting values.foreach..")
    ratings.values.foreach((rating) => println(rating))


  }
}
