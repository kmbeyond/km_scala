/**
  * Created by kiran on 2/28/17.
  */
object ScalaArrayDemo {

  def main(args: Array[String]) {

    var arr1:Array[String] = new Array[String](3)
    println("Array length="+arr1.length+"; size="+arr1.size) //Use length instead of size
    //or define as
    var arr2 = new Array[String](3)
    arr1(0) = "@0"; arr1(1) = "@1"; arr1(4/2) = "@2"

    println("All Elements of Array:")
    for ( x <- arr1 ) {
      println( x )
    }

    println("Numeric Array:")
    var numArray = Array(1.9, 2.9, 3.4, 3.5)
    println("Array length="+numArray.length+"; size="+numArray.size)

    for ( i <- 1 to (numArray.length - 1) ) {
      println("@"+i+" : "+numArray(i));
    }

    // Summing all elements
    var total = 0.0;
    for ( i <- 0 to (numArray.length - 1)) {
      total += numArray(i);
    }
    println("Total is " + total);


    System.out.println("All input arguments:")
    for ( i <- 1 to (args.length - 1) ) {
      println("@" + i + " : " + args(i));
    }

    println("2D Array/matrix:")
    var my2DMatrix:Array[Array[String]] = Array.ofDim[String](3,5)
    println("- Rows="+my2DMatrix.length+"; Columns="+my2DMatrix(0).length)

    for (i:Int <- 0 to my2DMatrix.length-1) {
      for ( j:Int <- 0 to my2DMatrix(i).length-1) {
        my2DMatrix(i)(j) = "("+i+","+j+")";
      }
    }

    println("Print two dimensional array")
    for (i <- 0 to my2DMatrix.length-1) {
      for ( j:Int <- 0 to my2DMatrix(i).length-1) {
        print(" *" + my2DMatrix(i)(j));
      }
      println();
    }

    //System.out.println("Options selected:")
    //for (var arg <- args)
      //if (arg.startsWith("-"))
      //  System.out.println(" "+arg.substring(1))
  }

}