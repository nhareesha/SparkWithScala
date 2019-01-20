package com.sparkscala.prep

import org.apache.spark.SparkContext

object LoadMultipleFiles {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "MultipleFiles")

    val contents = sc.textFile("file1.txt,file2.txt")
    val flatMapRdd = contents.flatMap(x => x.split(" "))
    val pairedRdd = flatMapRdd.map(x => (x, 1))
    val reducedRdd = pairedRdd.reduceByKey(_ + _)
    reducedRdd.take(10).foreach(println)

    val swappedRdd = reducedRdd.map(x => x.swap)
    swappedRdd.take(10).foreach(println)

    swappedRdd.sortByKey(false)




  }
}
