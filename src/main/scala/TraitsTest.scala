/**
  * Created by kiran on 2/25/17.
  */

trait Similarity {
  def isSimilar(x: Any): Boolean
  def isNotSimilar(x: Any): Boolean = !isSimilar(x)
}


class PointXY(xc: Int, yc: Int) extends Similarity {
  var x: Int = xc
  var y: Int = yc

  def isSimilar(obj: Any) =
    obj.isInstanceOf[PointXY] &&
      obj.asInstanceOf[PointXY].x == x

}

object TraitsTest extends App {

  val p1 = new PointXY(2, 3)
  val p2 = new PointXY(2, 4)
  val p3 = new PointXY(3, 3)
  val p4 = new PointXY(2, 3)

  println(p1.isSimilar(p2))
  println(p1.isSimilar(p3))
  // Point's isNotSimilar is defined in Similarity
  println(p1.isNotSimilar(2))
  println(p1.isNotSimilar(p4))
}
