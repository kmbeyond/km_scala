/**
  * Created by kiran on 2/27/17.
  */
object ScalaMapDemo {

  def main(args: Array[String]) {

    val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF", "peru" -> "#CD853F")

    val nums: Map[Int, Int] = Map()

    println( "Keys in colors Map: " + colors.keys )
    println( "Values in colors Map: " + colors.values )

    if( !colors.isEmpty )
      println( "colors is NOT empty." )

    if(nums.isEmpty)
      println( "nums is empty " + nums.isEmpty )

    println( "---------")
    println( "**** PRINT ALL ELEMENTS using foreach{}:")
    colors.keys.foreach{ i =>
      println( "Key= " + i + "; Value= " + colors(i) )
    }
    println( "---------")



    val colors1 = Map("red" -> "#FF0000", "azure" -> "#F0FFFF", "peru" -> "#CD853F")
    val colors2 = Map("blue" -> "#0033FF", "yellow" -> "#FFFF00", "red" -> "#FF0000")

    // use two or more Maps with ++ as operator
    var colors3 = colors1 ++ colors2
    println( "colors1 ++ colors2 : " + colors3 )

    // use two maps with ++ as method
    var colors4 = colors1.++(colors2)
    println( "colors1.++(colors2)) : " + colors4 )

    if( colors4.contains( "red" )) {
      println("Red key exists with value :"  + colors4("red"))
    } else {
      println("Red key does not exist")
    }



  }



}
