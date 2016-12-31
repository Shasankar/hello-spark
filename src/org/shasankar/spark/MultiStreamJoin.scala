// Join input from 2 streams
package org.shasankar.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming._

object MultiStreamJoin {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Multi Stream Join")
    val ssc = new StreamingContext(conf,Seconds(20))
    val ageStream = ssc.socketTextStream(args(0),args(1).toInt) // Same IP
    val addrsStream = ssc.socketTextStream(args(0),args(2).toInt) // different port
    val age = ageStream.map(_.split(" ")).map(tokens => (tokens(0),tokens(1)))
    val addrs = addrsStream.map(_.split(" ")).map(tokens => (tokens(0),tokens(1)))
    age.join(addrs).print()
    ssc.start()
    ssc.awaitTermination()
  }
}