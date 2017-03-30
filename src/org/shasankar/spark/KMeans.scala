// program to get K-Means in a cluster of 2D points
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
  def calcDist(points: ((Double, Double), (Double, Double))) = {
    val ((x1, y1), (x2, y2)) = points
    val dist = sqrt(pow((x1 - x2), 2) + pow((y1 - y2), 2))
    ((x1, y1), (x2, y2), dist)
  }
  def mkPointKey(centerPointDist: ((Double, Double), (Double, Double), Double)) = {
    val (center, point, dist) = centerPointDist
    (point, (center, dist))
  }
  def getClosestCenter(center1: ((Double, Double), Double), center2: ((Double, Double), Double)) = {
    if (center1._2 < center2._2) center1 else center2
  }
  def mkCenterKey(pointCenterDist: ((Double, Double), ((Double, Double), Double))) = {
    val (point, (center, dist)) = pointCenterDist
    (center, point)
  }
  def getNewCenter(point1: (Double, Double), point2: (Double, Double)) = {
    val (x1, y1) = point1
    val (x2, y2) = point2
    val x3 = (x1 + x2) / 2
    val y3 = (y1 + y2) / 2
    (x3, y3)
  }
  def main(args: Array[String]) {
    val Array(path, k, threshold) = parseArgs(args)
    val conf = new SparkConf().setAppName("KMeans").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val inputRdd = sc.textFile(path).map(parseInput _)
    var centresRdd = sc.parallelize(inputRdd.takeSample(false, k.toInt))
    var centersNewcentersDistRdd,crossThrshldRdd : RDD[((Double,Double),(Double,Double),Double)] = null
    do {
      centersNewcentersDistRdd = centresRdd.cartesian(inputRdd).map(calcDist _)           //calculate distance between each point and center points
                                  .map(mkPointKey _).reduceByKey(getClosestCenter _)      //determine closest center for each point
                                  .map(mkCenterKey _).reduceByKey(getNewCenter _)         //determine new center by taking mean of points closest to old center
                                  .map(calcDist _).cache()                                //get distance between new and old centers
      centresRdd = centersNewcentersDistRdd.map(_._2)                                     //get new centers for next iteration
      crossThrshldRdd = centersNewcentersDistRdd.filter(_._3 > threshold.toDouble)        //filter for distance between new and old centers > threshold value
    } while (!crossThrshldRdd.isEmpty)                                                    //if any distance greater than threshold continue next iteration
    centresRdd.collect().foreach(println)
  }
}