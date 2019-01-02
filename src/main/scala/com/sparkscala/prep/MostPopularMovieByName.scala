package com.sparkscala.prep

import java.nio.charset.CodingErrorAction

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
      val lineVals = line.split("|")
      if (lineVals.length > 1) {
        movieNames += (lineVals(0).toInt -> lineVals(1))
      }
    }
    movieNames
  }

}
