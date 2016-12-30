//Rolling word count of words coming in from the stream using updateStateByKey
package org.shasankar.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object RollingWordCount {
  def updateCount(newValues: Seq[Int], rollingCount: Option[Int]): Option[Int] = {
    var sum = rollingCount.getOrElse(0)
    newValues.foreach{value =>
      sum = value + sum
    }
    Some(sum)
  }
  
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("rolling word count")
    val ssc = new StreamingContext(conf,Seconds(10))
    val lines = ssc.socketTextStream(args(0),args(1).toInt)
    ssc.checkpoint(args(2)) //checkpoint file
    lines.flatMap(_.split(" ")).map((_,1)).updateStateByKey(updateCount).print()
    ssc.start()
    ssc.awaitTermination()
  }
}