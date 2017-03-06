import UserType.nextId

/**
  * Created by kiran on 2/28/17.
  */
object ScalaEnumDemo {
  def main(args: Array[String]) {
    var sToday = "TUE"

    if (sToday == Days.TUE.toString())
      println("Tuesday")
    else
      println("Not Tuesday")

    var iMonth = 3
    if("M"+iMonth == Months.M2.toString())
      println("February")
    else
      println("Not Frbruary")

    println(Months.M2.id)

    //using with ***NOT WORKING***
    /*val sMonth = (iMonth) match {
      case Months.M2.id => "February"
      case Months.M3.id => "March"
      case Months.M4.id => "April"
    }*/

      println(UserType.Anonymous.id)
  }
}

object Days extends Enumeration {
  type Days = Value
  val MON, TUE, WED, THU, FRI, SAT, SUN = Value
}

object Months extends Enumeration {
  type Months[Int] = Value
  nextId=100
  val M1, M2, M3, M4, M5, M6, M7, M8, M9, M10, M11, M12 = Value
}

object UserType extends Enumeration {
  type UserType = Value
  nextId=100
  val Anonymous, Member, Paid = Value
}


