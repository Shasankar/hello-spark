//Program to find total price of books by last name of the authors
//Input
//Speed love,Long book about love,Brian,Dog,10
//Long day,Story about Monday,Emily,Blue,20
//Flying Car,Novel about airplanes,Phil,High,5
//Short day,Novel about a day,Phil,Dog,30
//Output
//(Dog,40)
//(High,5)
//(Blue,20)

package org.shasankar.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object BookLastNmPrice {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Last Name Price Total").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val books = sc.textFile(args(0))
    val lastNmPrice = books.map(_.split(",")).map(f => (f(3), f(4).toInt)).reduceByKey(_ + _)
    lastNmPrice.saveAsTextFile(args(1))
  }
}