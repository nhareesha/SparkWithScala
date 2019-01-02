package com.sparkscala.prep

import org.apache.log4j._
import org.apache.spark.SparkContext

object WordCount {

  /**
    * Word count using flatMap
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "WordCount")
    val linesRdd = sc.textFile("../SparkScala/book.txt")

    println("Numebr of line" + linesRdd.count());

    // Takes each line and splits it based on the delimiter using flatMap
    var flatMapRdd1 = linesRdd.flatMap(line => line.split(" "))
    flatMapRdd1 = flatMapRdd1.filter(x => !x.contains("\n"))

    flatMapRdd1.take(10).foreach(println)

    // using reduceByKey adds up all the occurrence based on each key
    val aggWordCount1 = flatMapRdd1.map(x => (x, 1)).reduceByKey((v1, v2) => v1 + v2)

    aggWordCount1.collect().sortBy(x => x._2).foreach(println)

    /***** Better Word Count :  Where punctuations are taken care of *******/

    println("Better Word Count")
    val flatMapRdd2 = linesRdd.flatMap(line => line.split("\\W+"))
    val aggWordCount2 = flatMapRdd2.map(x => x.toLowerCase()).map(x => (x, 1)).reduceByKey((v1, v2) => v1 + v2)

    // Here values are collected to driver and sorted
    aggWordCount2.collect().sortBy(x => x._2).foreach(println)

    // sortBy() returns an RDD.
    //    aggWordCount2.sortBy(x => x._2).foreach(x => println(x._1 +" occured : "+x._2+" times"))
    aggWordCount2.map(x => (x._2,x._1)).sortByKey().collect().foreach(x => println(x._1 + " times : " + x._2 + " is present in the book"))

    /****** even better word count using stop list *******/

    val list = List("is","a","the","that","was","were","you","of","on","what","when","in","for")
    val aggWordCount3 = flatMapRdd2.filter(x => !list.contains(x)).map(x => x.toLowerCase()).map(x => (x,1)).reduceByKey((v1,v2) => v1+v2)

    aggWordCount3.map(x => (x._2,x._1)).sortByKey().collect().foreach(println)
  }

}
