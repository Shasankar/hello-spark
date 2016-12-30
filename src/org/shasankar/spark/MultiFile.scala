package org.shasankar.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object MultiFile {
  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local").setAppName("multipleFiles")
    val sc = new SparkContext(conf)
    val input = sc.wholeTextFiles(args(0)) //directory to read
    val result = input.mapValues{y=>
      val nums = y.split(" ").map(x => x.toDouble)
      nums.sum / nums.size.toDouble
    }
    /*can also be written as
    val result = input.mapValues(y=> {
      val nums = y.split(" ").map(x => x.toDouble)
      nums.sum / nums.size.toDouble
    }
    )*/
    result.saveAsTextFile(args(1))
  }
}