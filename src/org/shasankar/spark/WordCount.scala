package org.shasankar.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object WordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Word Count").setMaster("local[*]")
    val sc = new SparkContext(conf)
    println(sc.defaultParallelism)
    val myFile = sc.textFile(args(0),4)
    println(myFile.partitions.size)
    myFile.flatMap(_.split(" ")).map((_,1))
            .reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))
            //foreach((w) => println(w._1 + " --> " + w._2))
    val totalCount = myFile.flatMap(_.split(" ")).map((_,1)).reduce((a,b) => (a._1,a._2+b._2))
    println("This is totalCount -->" + totalCount)
  }
}