package com.sparkscala.prep

import org.apache.spark.SparkContext

/**
  * Running Average friends By Age
  */
object AverageFriendsByAge {

  def main(args:Array[String]):Unit={
    val sc = new SparkContext("local[*]","AverageFriendsByAge")

    // textFile returns an RDD of String
    val rawRdd = sc.textFile("../SparkScala/fakefriends.csv")

    rawRdd.take(10).foreach(println) // check the out put

    // map to construct  (age, numberOfFriends) tuple
    val ageFrndsTuple = rawRdd.map( x => (x.split(",")(2).toInt,x.split(",")(3).toInt))

    ageFrndsTuple.take(10).foreach(println) // check the output

    // Need sum of all friends for a given age and number of instances of that age
    // mapValues acts on values - Here v is the value part of the tuple
    val ageFrnds2 = ageFrndsTuple.mapValues(v => (v,1))

    ageFrnds2.take(10).foreach(println)

    // For each age find the total of the friends
    // reduceByKey operates on the each value tuple for a given Key which is age here
    // x,y are the value tuples
    val aggAgeFriends = ageFrnds2.reduceByKey((x,y) => ((x._1+y._1),(x._2+y._2)))

    aggAgeFriends.take(10).foreach(println) // check output

    // Calculate Average number of friends  . This is for each given age
    // mapByValues is on value.
    val avgFrndsByAgeTuples = aggAgeFriends.mapValues(x => x._1/x._2)

    avgFrndsByAgeTuples.take(10).foreach(println)

    // collect returns an Array
    avgFrndsByAgeTuples.collect().sortBy(x => x._1).foreach(println)


  }

}
/*
// OUTPUT

0,Will,33,385
1,Jean-Luc,26,2
2,Hugh,55,221
3,Deanna,40,465
4,Quark,68,21
5,Weyoun,59,318
6,Gowron,37,220
7,Will,54,307
8,Jadzia,38,380


(33,385)
(26,2)
(55,221)
(40,465)
(68,21)
(59,318)
(37,220)
(54,307)
(38,380)
(27,181)


(33,(385,1))
(26,(2,1))
(55,(221,1))
(40,(465,1))
(68,(21,1))
(59,(318,1))
(37,(220,1))
(54,(307,1))
(38,(380,1))
(27,(181,1))


(34,(1473,6))
(52,(3747,11))
(56,(1840,6))
(66,(2488,9))
(22,(1445,7))
(28,(2091,10))
(54,(3615,13))
(46,(2908,13))
(48,(2814,10))
(30,(2594,11))

(34,245)
(52,340)
(56,306)
(66,276)
(22,206)
(28,209)
(54,278)
(46,223)
(48,281)
(30,235)


(18,343)
(19,213)
(20,165)
(21,350)
(22,206)
(23,246)
(24,233)
(25,197)
(26,242)
(27,228)
(28,209)
(29,215)
(30,235)
(31,267)
(32,207)
(33,325)
(34,245)
(35,211)
(36,246)
(37,249)
(38,193)
(39,169)
(40,250)
(41,268)
(42,303)
(43,230)
(44,282)
(45,309)
(46,223)
(47,233)
(48,281)
(49,184)
(50,254)
(51,302)
(52,340)
(53,222)
(54,278)
(55,295)
(56,306)
(57,258)
(58,116)
(59,220)
(60,202)
(61,256)
(62,220)
(63,384)
(64,281)
(65,298)
(66,276)
(67,214)
(68,269)
(69,235)

*/