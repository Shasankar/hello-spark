//Demo distinct, union, intersection, subtract
//Problem1: Get all the states from where employees belong to in the 2 companies
//Problem2: Get all the common states from where employees belong to in the 2 companies
//Problem3: Get all the states present in file1 but not in file2
//Input
//file1                          file2
//Sales,Trainee,12000,UP		    Sales,Trainee,12000,UP	
//Sales,Lead,32000,AP			      Sales,Lead,32000,AP	
//Sales,Lead,32000,LA			      Sales,Lead,32000,LA	
//Sales,Lead,32000,MH			      Sales,Lead,32000,LA	
//Sales,Lead,32000,AP			      Sales,Lead,32000,LA	
//Sales,Lead,32000,TN			      Marketing,Associate,19000,TN	
//Sales,Lead,32000,LA			      Marketing,Associate,19000,WB	
//Sales,Lead,32000,LA			      Marketing,Associate,19000,OR	
//Marketing,Associate,18000,TN	HR,Manager,58000,TN	
//Marketing,Associate,18000,TN		
//HR,Manager,58000,TN			

package org.shasankar.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object TwoCompaniesAggr {
  def main(args: Array[String]){
    val conf = new SparkConf().setMaster("local[*]").setAppName("Two Companies Aggregation")
    val sc = new SparkContext(conf)
    val comp1 = sc.textFile(args(0)).persist()
    val comp1States = comp1.map(_.split(",")).map(_(3))
    val comp2 = sc.textFile(args(1)).persist()
    val comp2States = comp2.map(_.split(",")).map(_(3))
    val allStates = comp1States.union(comp2States).distinct
    allStates.saveAsTextFile(args(2))
    val commStates = comp1States.intersection(comp2States)
    commStates.saveAsTextFile(args(3))
    val file1MismatchStates = comp1States.subtract(comp2States)
    file1MismatchStates.saveAsTextFile(args(4))
  }
}