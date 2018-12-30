package com.sparkscala.prep

import java.util.logging.{Level, Logger}

import org.apache.spark.SparkContext;



/**
  * Scala Singleton object to create
  */
object MovieRating {

  def main(arags:Array[String]):Unit={

    Logger.getLogger("org").setLevel(Level.WARNING)

    val sc = new SparkContext("local[*]","RatingCounter")

    val linesRdd = sc.textFile("../ml-100k/u.data")   // sparkContext load as a textFile into RDD

    linesRdd.take(10).foreach(println)  // take , takes the first n number of rows from the RDD

    println(linesRdd.count())    // prints the count of the nuber of rows from the RDD

    // map function on RDD return another RDD . Getting Ratings part of the line.
    val ratingRdd = linesRdd.map((x : String) => x.split("\t")(2))

     ratingRdd.sample(false,0.1).take(10).foreach(println)

    // take is an action and it returns and Array.foreach is on Array
    ratingRdd.take(10).foreach(println)

    // countByValue returns tuples of (Key / Value) pairs
    val ratingMap = ratingRdd.countByValue()

    // foreach acts on each tuple of map
    ratingMap.foreach(x => println(x._2));
    ratingMap.foreach(println);

    //Sort by Key
    val sortedMap = ratingMap.toSeq.sortBy(_._2)

    sortedMap.foreach(println)


    // Use reduceByKey instead of countbyValue, when dataset is large

    val parRdd = ratingRdd.map(x => (x,1L)).reduceByKey((x,y) => x+y)   // PairRDDFunction

    parRdd.foreach(x => println(x._1 + "->"+x._2))
  }

}
