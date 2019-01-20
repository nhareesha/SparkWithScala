package com.sparkscala.prep

import org.apache.spark.SparkContext

object JoinAndSave{
  def main(args : Array[String]):Unit={
    val sc = new SparkContext("local[*]","JoinAndSave")

    //load csv as text file
    val namescsv = sc.textFile("names.csv")

    // convert into paired RDD
    val pairRddNames = namescsv.map(x => (x.split(",")(0),x.split(",")(1)))
    namescsv.take(10).foreach(println)

    val salrycsv = sc.textFile("salary.csv")
    val joinedRddSalary = salrycsv.map(x => (x.split(",")(0),x.split(",")(1)))
    salrycsv.take(10).foreach(println)

    // join rdds
    val joinedRdd = pairRddNames.join(joinedRddSalary)
    joinedRdd.foreach(println)

    // drop keys and just have values
    val valuesRdd = joinedRdd.values

    // flip key/values
    val swappedRdd = valuesRdd.map(x => x.swap)
    swappedRdd.foreach(println)

    // groupByKeys returns [key, Iterable[]]
    val groupedRdd = swappedRdd.groupByKey()
    groupedRdd.foreach(println)

    // Map each values to a RDD using makeRDD
    val rddByKey = groupedRdd.map{ case (k,v) => k -> sc.makeRDD(v.toSeq)}
    rddByKey.foreach {case (k,rdd) => rdd.saveAsTextFile("/output"+k)}



  }

}
