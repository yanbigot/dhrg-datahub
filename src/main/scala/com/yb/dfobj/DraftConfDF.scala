package com.yb.dfobj

import net.liftweb.json.Extraction.decompose
import net.liftweb.json.JsonAST.render
import net.liftweb.json.Printer.compact
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataTypes.createMapType
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DraftConfDF {
//  val jsonConfFilePath = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\conf-inline.json"
  val jsonConfFilePath = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\draftConfiguration.json"
  val confSchema =
    StructType(
      Array(
        StructField("entity", StringType, true),
        StructField("needs", StringType, true),
        StructField("mapping", createMapType(StringType, StringType), true)
      )
    )

  implicit val formats = net.liftweb.json.DefaultFormats

  def extract(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.sqlContext.read
      .schema(confSchema)
      .option("multiline", "true")
      .json(jsonConfFilePath)
      .withColumn("mappingAsJson", mapToJsonUDF($"mapping"))
      .drop($"mapping")
  }

  def mapToJson(m: Map[String, String]): String ={
    compact(render(decompose(m)))
  }
  val mapToJsonUDF = udf[String,Map[String, String]](mapToJson)
}
