/**
  * Created by kiran on 2/27/17.
  */

object ScalaListDemo {

  def main(args: Array[String]) {

    // List of Strings
    var fruit1: List[String] = List("apples", "oranges", "pears")

    val fruit2 = "apples" :: ("oranges" :: ("pears" :: Nil))

    // List of Integers
    val nums: List[Int] = List(1, 2, 3, 4)

    val nums2 = 1 :: (2 :: (3 :: (4 :: Nil)))

    // Empty List.
    val empty: List[Nothing] = List()

    // Empty List.
    //val empty = Nil

    println( "fruit : " + fruit1 )


    // Two dimensional list
    val dimList: List[List[Int]] =
      List(
        List(1, 0, 0),
        List(0, 1, 0),
        List(0, 0, 1)
      )
    println( "2dim : " + dimList )


    // Two dimensional list
    val dimList2 = (1 :: (0 :: (0 :: Nil))) ::
      (0 :: (1 :: (0 :: Nil))) ::
      (0 :: (0 :: (1 :: Nil))) :: Nil
    println( "2dim : " + dimList2 )


    val fruit3 = "apples" :: ("oranges" :: ("pears" :: Nil))
    val fruit4 = "mangoes" :: ("banana" :: Nil)

    // use two or more lists with ::: operator
    var fruit34 = fruit3 ::: fruit4
    println( "fruit3 ::: fruit4 : " + fruit34 )

    // use two lists with Set.:::() method
    var fruit12 = fruit1 .:::(fruit2)
    println( "fruit1.:::(fruit2) : " + fruit12 )

    //Concatenate two or more lists
    var fruit5 = List.concat(fruit1, fruit2)
    println( "List.concat(fruit1, fruit2) : " + fruit5  )


    // Creates 5 elements using the given function.
    val squaresList = List.tabulate(6)(n => n * n)
    println( "squares : " + squaresList  )

    val multList = List.tabulate( 4,5 )( _ * _ )
    println( "mul : " + multList  )

    println("tabulate()...")
    // Creates 5 elements using the given function.
    val squares = List.tabulate(6)(n => n * n)
    println( "squares : " + squares  )

    val mul = List.tabulate( 4,5 )( _ * _ )
    println( "mul : " + mul  )



    println( "Before reverse fruit : " + fruit1 )
    println( "After reverse fruit : " + fruit1.reverse )

  }
}