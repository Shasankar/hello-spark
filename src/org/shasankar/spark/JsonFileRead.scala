package org.shasankar.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature

object JsonFileRead {
  case class Pkg(name: String, version: String, description: String, main: String)
  case class Out(name: String, desc: String)
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("jsonFileRead")
    val sc = new SparkContext(conf)
    val input = sc.textFile(args(0))

    val parsed = input.flatMap(rec => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.registerModule(DefaultScalaModule)
      try {
        Some(mapper.readValue(rec, classOf[Pkg]))
      } catch {
        case e: Exception => None
      }
    })
    parsed.map(rec => (rec.name, rec.description)).saveAsTextFile(args(1))
    parsed.map(rec => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      mapper.writeValueAsString(rec)
    })
      .saveAsTextFile(args(2))
    parsed.map(rec => {
      val mapper = new ObjectMapper with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      mapper.writeValueAsString(Out(rec.name,rec.description))
    })
      .saveAsTextFile(args(3))
  }
}