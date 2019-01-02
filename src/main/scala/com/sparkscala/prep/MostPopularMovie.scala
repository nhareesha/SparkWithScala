package com.sparkscala.prep

import org.apache.spark.SparkContext

/**
  * Find the most popular movie - Number of times a movie watched
  *
  * UserId MovieId rating timeStamp
  * 196	242	3	881250949
  * 186	302	3	891717742
  * 22	377	1	878887116
  * 244	51	2	880606923
  * 166	346	1	886397596
  * 298	474	4	884182806
  */
object MostPopularMovie {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "PopularMovie")
    val linesRdd = sc.textFile("../ml-100k/u.data")

    // Extract movieId from  each line of RDD
    val movieIdRdd = linesRdd.map(x => x.split("\t")(1).toInt)
    movieIdRdd.take(10).foreach(println)

    // Map each movie id to 1 .Sum up all the values to get the number of occurrences - (movieId,1)
    val popularMovieIds = movieIdRdd.map(x => (x,1)).reduceByKey((v1,v2) => v1+v2)

    // In order to use sortByKey() transforamtion for sorting flip (movieId, count) to (count, movieId)
    val sortedPopularMovieIds = popularMovieIds.map(x => (x._2,x._1)).sortByKey()

    // collect and print
    sortedPopularMovieIds.collect().foreach(println)


  }

}
