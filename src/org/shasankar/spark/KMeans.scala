package org.shasankar.spark

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import math._

object KMeans {
  def parseArgs(args: Array[String]) = {
    val usage = """Usage: KMeans <input file> <K> <threshold>
                    | <input file> path to the input file
                    | <K> value of K or no. of clusters
                    | <threshold> threshold value for switching clusters""".stripMargin
    args.length match {
      case 3 => args
      case _ => {
        System.err.println(usage)
        System.exit(1)
        args
      }
    }
  }
  def parseInput(line: String) = {
    val Array(x, y) = line.split(",")
    (x.toDouble, y.toDouble)
  }
  def calcDist(points: ((Double,Double),(Double,Double))) = {
    val ((x1,y1),(x2,y2)) = points
    val dist = sqrt(pow((x1 - x2), 2) + pow((y1 - y2), 2))
    ((x1, y1), (x2, y2), dist)
  }
  def main(args: Array[String]) {
    val Array(path, k, threshold) = parseArgs(args)
    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputRdd = sc.textFile(path).map(parseInput _)
    val onePointRdd = sc.parallelize(Seq(inputRdd.first()))
    val farthestPointsRdd = sc.parallelize(onePointRdd.cartesian(inputRdd).map(calcDist _).sortBy(_._3,false)
                        .take(k.toInt - 1).map(_._2)).union(onePointRdd)
    farthestPointsRdd.collect().foreach(println)
  }
}