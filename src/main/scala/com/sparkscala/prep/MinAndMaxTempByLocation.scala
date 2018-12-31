package com.sparkscala.prep

import org.apache.spark.SparkContext
import scala.math._
import org.apache.log4j._

object MinAndMaxTempByLocation {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "MinTemperatureByLoc")

    val linesRdd = sc.textFile("../SparkScala/1800.csv")

    linesRdd.take(10).foreach(println) //sample output

    /******** FIND MIN temperature  **********/

    // Filter all the lines that has TMIN temperature
    val minTempRdd = linesRdd.filter(line => line.split(",")(2).equals("TMIN"))

    minTempRdd.take(10).foreach(println) //sample output

    // map each line to a tuple of (station,tempReading)
    val minTempTupleRdd = minTempRdd.map(line => (line.split(",")(0), line.split(",")(3).toFloat))

    minTempTupleRdd.take(10).foreach(println) //sample output

    //find min temperature for a given location , use reduceByKey to find min temperature for a given Key
    // here x,y are not key, value pair.They are values of a given key
    val minTempTuples = minTempTupleRdd.reduceByKey((x,y) => min(x,y))

    minTempTuples.take(10).foreach(println) //sample output

    println("MIN temperature By Location : ")
    minTempTuples.collect().sortBy(x => x._2).foreach(println)

    /********FIND MAX temperature  **********/

    val maxTempLinesRdd = linesRdd.filter(line => line.split(",")(2).equals("TMAX"))

    val maxTempTuples = maxTempLinesRdd.map(line => (line.split(",")(0),line.split(",")(3).toFloat))

    val maxTempByLoc = maxTempTuples.reduceByKey((x,y) => max(x,y))

    println("MAX temperature By Location : ")
    maxTempByLoc.collect().sortBy(x => x._2).foreach(println)

  }

  /**
    * Compares temp1 and temp2 and return the minimum temperature
    * @param temp1
    * @param temp2
    * @return
    */
  def findMinTemp(temp1: Float, temp2: Float): Float = {
    if(temp1 > temp2)
      temp2
    else if(temp1<temp2)
      temp1
    else
      temp1
  }

}
