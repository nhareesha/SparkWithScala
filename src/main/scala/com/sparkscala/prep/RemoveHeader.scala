package com.sparkscala.prep

import org.apache.spark.SparkContext

object RemoveHeader {

  def main(args : Array[String]): Unit ={
  val sc = new SparkContext("local[*]","RemoveHeader")
  val csv = sc.textFile("MovieHits.csv")

  // split based on the new line
  val fmap = csv.flatMap(x => x.split("\n"))

    fmap.foreach(println)

    // get header
    val header = fmap.first()

    fmap.filter(x => !x.contains(header)).foreach(println)

    // using different way of representation
    fmap.filter{case x => !x.contains(header)}.foreach(println)


  }

}
