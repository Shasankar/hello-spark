package org.shasankar.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Hamlet {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Hamlet").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val pageCount = sc.textFile(args(0))
    val first10 = pageCount.take(10)
    first10.foreach(println)
    println("******formatted*****")
    for(i <- 1 to first10.length) println("Line " + i + ": " + first10(i-1))
    val pgCount = pageCount.count
    println(pgCount)
    val linesWithThis = pageCount.filter(_.contains("this"))
    println("===========Lines With This=========")
    linesWithThis.foreach(println)
    println("===================================")
    pageCount.map(_.split(" ")).map(_.length).sortBy(c => c,false).foreach(println)
  }
}