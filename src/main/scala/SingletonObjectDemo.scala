/**
  * Created by kiran on 2/1/17.
  */

import java.io._

class Point2(val xc: Int, val yc: Int) {
  var x: Int = xc
  var y: Int = yc

  def move(dx: Int, dy: Int) {
    x = x + dx
    y = y + dy
    println ("Point x location : " + x);
    println ("Point y location : " + y);
  }
}

object SingletonObjectDemo {
  def main(args: Array[String]) {
    val point = new Point2(10, 20)
    printPoint

    def printPoint{
      println ("Point x location : " + point.x);
      println ("Point y location : " + point.y);
    }
  }
}

