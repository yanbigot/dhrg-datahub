package com.yb.dfobj

import com.yb.dfobj.functions.CombineMaps
import org.apache.spark.sql.functions.{collect_list, concat_ws, udf}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object PreDraftDF {


  def transform(srcView: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val combineMaps = new CombineMaps[String, String](StringType, StringType, _ + _)

    srcView.printSchema()

    srcView
      .groupBy("sourceKey")
      .agg(
        concat_ws(",", collect_list($"jsonDataMap")).as("jsonDataMap"),
        combineMaps($"dataMap").as("dataMap"),
        concat_ws(",", collect_list($"blockId")).as("composedBy")
      )
      .withColumn("jsonDataMap", makeValidJsonUDF($"jsonDataMap"))
  }

  def makeValidJson( dataMap: String ): String = {
    """{"entity":[""" + dataMap +"""]}"""
  }
  val makeValidJsonUDF = udf[String, String](makeValidJson)
}
