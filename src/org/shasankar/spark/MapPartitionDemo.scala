package org.shasankar.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object MapPartitionDemo {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]","Map Partitions Demo")
    val input = sc.textFile(args(0))
    input.mapPartitionsWithIndex((index,records) => {
      records.map(record => "PartitionNo.: " + index + " | " + record)
    }).saveAsTextFile(args(1))
  }
}