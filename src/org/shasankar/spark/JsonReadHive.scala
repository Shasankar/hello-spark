package org.shasankar.spark
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object JsonReadHive {
  def main(args: Array[String]){
    val sc = new SparkContext("local","jsonReadHive",new SparkConf)
    val hc = new HiveContext(sc)
    val input = hc.jsonFile(args(0))
    input.registerTempTable("Packages")
    hc.sql("select name,version from Packages").write.csv(args(1))
  }
}