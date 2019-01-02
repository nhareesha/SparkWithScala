package com.sparkscala.prep

import org.apache.spark.SparkContext

object TotalAmountSpentByCustomer {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]","TotalAmountSpentByCustomers")
    val ordersRdd = sc.textFile("../SparkScala/customer-orders.csv")
    ordersRdd.take(10).foreach(println)
    val  itemAmountRdd = ordersRdd.map(x =>(x.split(",")(0),x.split(",")(2).toFloat))
    val aggItemAmountRdd = itemAmountRdd.reduceByKey((v1,v2) => v1+v2)
    //SortBy total amount spent
    aggItemAmountRdd.map(x => (x._2,x._1)).sortByKey().foreach(println)
  }
}
