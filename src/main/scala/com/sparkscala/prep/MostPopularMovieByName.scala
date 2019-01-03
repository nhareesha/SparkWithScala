package com.sparkscala.prep

import java.nio.charset.CodingErrorAction

import org.apache.spark.SparkContext

import scala.io.{Codec, Source}

/**
  * Find the most popular movie or movie that is more watched by Name
  */
object MostPopularMovieByName {

  // Loading (movieId -> MovieName) tuples as Map
  def loadMovieNames(): Map[Int, String] = {

    // Handle character encoding issues
    implicit val codec = Codec("UTF-8")
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    codec.onMalformedInput(CodingErrorAction.REPLACE)

    var movieNames: Map[Int, String] = Map()
    val lines = Source.fromFile("../ml-100k/u.item").getLines()
    for (line <- lines) {
      var lineVals = line.split('|')
      if (lineVals.length > 1) {
        movieNames += (lineVals(0).toInt -> lineVals(1))
      }
    }
    return movieNames
  }

  def main(args: Array[String]):Unit = {
    val sc = new SparkContext("local[*]","PopularMovieByName")

    // broadcast movieId map
    // Doing this, the result of loadMovieNames is available to all the executors
    var bcMovieNames = sc.broadcast(loadMovieNames)

    val movieInfoRdd = sc.textFile("../ml-100k/u.data")

    // from movieInfoRdd map to (movieId,1)
    val movieIdRdd = movieInfoRdd.map(x => (x.split("\t")(1).toInt , 1))

    val popularMovieRdd = movieIdRdd.reduceByKey((v1,v2) => v1+v2)

    // map (MovieId, count) to (count, movieId)
    val flipped = popularMovieRdd.map(x => (x._2,x._1))

    // sort (count,movieId) tuples based on the key count
    val sortedPopularMovieIds = flipped.sortByKey();

    // map (count.movieId) to get (movieName, count) - Use broadcasted map as lookup data to get movieName for given movieId
    val popularMoviesByName = sortedPopularMovieIds.map(x => (bcMovieNames.value.getOrElse(x._2,null) ,x._1))

    // collect and print popular movies
    popularMoviesByName.collect().foreach(println)
  }

}
