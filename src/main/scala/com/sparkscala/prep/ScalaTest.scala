package com.sparkscala.prep

object ScalaTest {

  def main(args: Array[String]): Unit = {
    val mapEx = Map(1->"one",2->"two",3->"three")
    // Size of map elements
    println(mapEx.size)

    // Iterate over map
    mapEx.foreach(x  => println(x._1 +"="+x._2))

    mapEx.foreach{case(k,v) => println(k + " " + v)}

  }
}
