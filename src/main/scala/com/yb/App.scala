package com.yb

import com.josephpconley.jsonpath.JSONPath
import com.yb.dfobj.{PreDraftDF, SourceViewDF}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import play.api.libs.json.Json

object App {
  val filePath     = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\sirhj-n-card.pos"
  val jsonFilePath = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\sirhj-n-card.json"

  case class Gr()

  def sparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Simple Spark Application")
      .master("local[*]")
      .getOrCreate()
    spark
  }

  def main( args: Array[String] ): Unit = {
    val spark: SparkSession = sparkSession
    import spark.implicits._

    val srcView = SourceViewDF.extract(spark);                                    srcView.show(false)
    val preDraftDf = PreDraftDF.transform(srcView, spark);                        preDraftDf.show(false)
//    val draftConfDf = DraftConfDF.extract(spark);                                 draftConfDf.show(false)
//    val draftDF =DraftDF.merge(preDraftDf, draftConfDf, spark);                   draftDF.show(false)
//
//    preDraftDf.printSchema()

    //pb remains : how to explode
    preDraftDf
      .where($"composedBy" startsWith "IDENTIFICATION")
      .withColumn("testNAME", $"dataMap"("name"))
      .show(false)

    //un(draftDF, spark)
    spark.stop()
  }

  def fetchFields(name: String, dataMap: String): Unit ={

  }

  def getJsonObjectByPath(dataMap: String, jPath: String): String ={
    val jsonDataMap = Json.parse(dataMap)
    JSONPath.query(jPath, jsonDataMap).toString()
  }

  /**
    * Create one DataFrame on a column value
    * Then union then all
    * This prepares a Draft oriented querying
    * WORKS :)
    * @param draftDF
    * @param spark
    */
  def un(draftDF: DataFrame, spark: SparkSession): Unit ={
    import spark.implicits._
    val distinctComposedBy =
      draftDF
        .select($"blockId")
        .distinct()
        .map(r => r.getString(0)).collect.toList

    val unioned = distinctComposedBy
      .map(compBy =>
        draftDF.where($"blockId" === compBy)
      )
      .reduce(_.union(_))

    unioned.withColumn("control", functions.lit("UNIONED")).show()
  }
}
