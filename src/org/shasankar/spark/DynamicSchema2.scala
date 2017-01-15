//get schema from schema file
//schema.txt
//name:string
//age:integer

package org.shasankar.spark

import scala.io.Source

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DataType._

object DynamicSchema2 {
  def main(args: Array[String]) {
    val inputFile = args(0)
    val schemaFile = args(1)
    val schemaLines = Source.fromFile(schemaFile, "UTF-8").getLines().map(_.split(":")).map(l => l(0) -> l(1)).toMap
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Dynamic Schema")
      .getOrCreate()
    import spark.implicits._
    //broadcast the schema to workers
    val schema = spark.sparkContext.broadcast(schemaLines)
    
    //schema -> DataType Map
    val nameToType = {
      Seq(IntegerType,StringType)
        .map(t => t.typeName -> t).toMap
    }
    //convert schema map to Seq of StructField's
    val fields = schema.value
      .map(field => StructField(field._1, nameToType(field._2), nullable = true)).toSeq
    val schemaStruct = StructType(fields)
    //read in the input file using the shema
    val input = spark.read.schema(schemaStruct).csv(inputFile)
    input.printSchema()
    // Creates a temporary view using the DataFrame
    input.createOrReplaceTempView("people")
    // SQL can be run over a temporary view created using DataFrames
    val results = spark.sql("SELECT name FROM people")
    results.show()
  }
}