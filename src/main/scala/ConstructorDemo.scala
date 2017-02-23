/**
  * Created by kiran on 2/1/17.
  */

class Person(val firstName: String, val lastName: String) {

  println("the constructor begins")
  val fullName = firstName + " " + lastName

  val HOME = System.getProperty("user.home");

  // define some methods
  def foo { println("foo") }
  def printFullName {
    // access the fullName field, which is created above
    println(fullName)
  }

  printFullName
  println("still in the constructor")

}

object ConstructorDemo{
  def main(args: Array[String]) {
    val p = new Person("Alvin", "Alexander")
  }
}