package com.yb.dfobj

import com.josephpconley.jsonpath.JSONPath
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.Json
import scala.util.parsing.json.JSON

object DraftDF {

  def merge(preDraftDf: DataFrame, draftConfDf: DataFrame, spark: SparkSession): DataFrame ={
    import spark.implicits._
    val draftDF = preDraftDf
      .join(draftConfDf, preDraftDf.col("composedBy") === draftConfDf.col("needs"))
      .withColumn("degreeDataMap",
        buildDraftFromConfUDF($"jsonDataMap", $"mappingAsJson")
      )

    draftDF
  }

  def buildDraftFromConf( dataMap: String, jsonConf: String): String = {
    val jsonConfMap = mapFromJson(jsonConf)
    val jsonData = Json.parse(dataMap)

    jsonConfMap
      .map(
        confEntry => (
          toStringProperty(confEntry._1),
          JSONPath.query(confEntry._2, jsonData)
        )
      ).map(kv =>
      toStringProperty(kv._1) + ":" + kv._2.toString
    ).mkString("{", ",", "}")
    //      ).mkString("{\"entity\": [", ",", "]}")
  }
  val buildDraftFromConfUDF = udf[String, String, String](buildDraftFromConf)
  def toStringProperty( s: String ): String = {
    "\"" + s + "\""
  }
  def mapFromJson( jsonString: String ): Map[String, String] = {
    JSON.parseFull(jsonString) match {
      case Some(jsonStringResult) =>
        println(jsonStringResult)
        jsonStringResult.asInstanceOf[Map[String, String]]
      case None => Map[String, String]()
    }
  }
}
