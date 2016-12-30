package org.shasankar.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCountJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Word Count Join").setMaster("local")
    val sc = new SparkContext(conf)
    val readme = sc.textFile(args(0)).flatMap(_.split(" ")).filter(_ == "Spark").map((_,1)).reduceByKey(_+_)
    val changes = sc.textFile(args(1)).flatMap(_.split(" ")).filter(_ == "Spark").map((_,1)).reduceByKey(_+_)
    readme.join(changes).saveAsTextFile(args(2))
  }
}