//Streaming Word Count
//commandLineParameters: <IP> <port>
package org.shasankar.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object HelloStreaming {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Hello Streaming")
    val ssc = new StreamingContext(conf,Seconds(5))
    val lines = ssc.socketTextStream(args(0),args(1).toInt)
    lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}