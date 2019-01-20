package com.sparkscala.prep

import org.apache.spark.SparkContext

object PairsCount {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "PairsCount")

    val courseRdd = sc.makeRDD(Seq(("Spark", 100), ("Hadoop", 200), ("Spark", 100), ("Hive", 300)))

    courseRdd.collect().foreach(println)

    courseRdd.map{ case x => (x,1)}.reduceByKey(_+_).filter{ case(item,count) => count > 1} .collect().foreach(println)

    courseRdd.map{ case x => (x,1)}.foldByKey(0)(_+_).filter{ case(item,count) => count > 1} .collect().foreach(println)

  }
}
