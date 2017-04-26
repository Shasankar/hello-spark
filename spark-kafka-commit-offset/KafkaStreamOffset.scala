package com.shas.sparkcode

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.Properties
import org.apache.spark.TaskContext

object KafkaStreamOffset {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GetCab").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(30))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_consumer",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("topicA")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { partition =>
        //val partitionOffsetRanges = offsetRanges(TaskContext.get.partitionId)
        val props = new Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("acks", "all")
        props.put("retries", new Integer(0))
        //props.put("batch.size", 16384)
        props.put("linger.ms", new Integer(1))
        //props.put("buffer.memory", 33554432)
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(record => {
          val message = new ProducerRecord[String, String]("topicB", record.key(), record.value())
          producer.send(message).get()
        })
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }
    stream.map(_.value).print()

    ssc.start()
    ssc.awaitTermination()
  }
}