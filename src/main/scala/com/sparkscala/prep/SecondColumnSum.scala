package com.sparkscala.prep

import org.apache.spark.SparkContext

object SecondColumnSum {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "SecondColumnSum")

    val txtrdd1 = sc.textFile("JoinFile1.txt")
    val txtrdd2 = sc.textFile("JoinFile2.txt")

    val rdd1 = txtrdd1.map{ case line => line.split(",", -1)
    match {
      case Array(a, b, c) => (a, (b, c))
    }
    }

    val rdd2 = txtrdd2.map{
      case line => line.split(",",-1)
        match{
        case Array(a,b,c) => (a,(b,c))
      }
    }

    println("ZipWithIndex")
    rdd2.zipWithIndex().collect().toMap.foreach(println)

    val joinrdd = rdd1.join(rdd2)


    val value = joinrdd.map{
      case (_,((_,_),(_,v4)))=>v4.toInt
    }.reduce(_+_)

    println(value)

  }

}