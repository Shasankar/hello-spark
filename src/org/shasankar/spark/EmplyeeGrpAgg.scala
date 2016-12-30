//Program to find totalCTC & countOfEmployee for a given dept,designation & state
//Input
//Sales,Trainee,12000,UP
//Sales,Lead,32000,AP
//Sales,Lead,32000,LA
//Sales,Lead,32000,TN
//Sales,Lead,32000,AP
//Sales,Lead,32000,TN
//Sales,Lead,32000,LA
//Sales,Lead,32000,LA
//Marketing,Associate,18000,TN
//Marketing,Associate,18000,TN
//HR,Manager,58000,TN
//Output
//((Sales,Lead,AP),(64000,2))
//((Sales,Trainee,UP),(12000,1))
//((HR,Manager,TN),(58000,1))
//((Sales,Lead,LA),(96000,3))
//((Marketing,Associate,TN),(36000,2))
//((Sales,Lead,TN),(64000,2))

package org.shasankar.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object EmplyeeGrpAgg {
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Employee group aggregation").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val employee = sc.textFile(args(0))
    val aggrEmp = employee.map(_.split(",")).map(rec => ((rec(0),rec(1),rec(3)),(rec(2).toInt,1)))
                          .reduceByKey((a,b) => ( a._1 + b._1, a._2 + b._2 ))
    aggrEmp.saveAsTextFile(args(1))
  }
}