package com.yb.dfobj

import java.util.Calendar

import com.yb.App.{filePath, sparkSession}
import org.apache.spark.sql.functions.{lit, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SourceViewDF {

  case class SourceView( blockId: String, sourceKey: String, dataMap: Map[String, String], jsonDataMap: String )
  case class BlockConf( position: Int, name: String, isSourceFileKey: Boolean = false, jsonRoot: Boolean = false )

  val idenfificationConf =
    Array(BlockConf(0, "block", jsonRoot = true), BlockConf(1, "igg", isSourceFileKey = true), BlockConf(2, "name"), BlockConf(3, "birthDate"), BlockConf(4, "tag"))
  val nationalityConf =
    Array(BlockConf(0, "block", jsonRoot = true), BlockConf(1, "igg", isSourceFileKey = true), BlockConf(2, "nationality"), BlockConf(3, "tag"))
  val contractConf =
    Array(BlockConf(0, "block", jsonRoot = true), BlockConf(1, "igg", isSourceFileKey = true), BlockConf(2, "homeHost", isSourceFileKey = true), BlockConf(3, "startDate"), BlockConf(4, "tag"))
  val jobConf =
    Array(BlockConf(0, "block", jsonRoot = true), BlockConf(1, "igg", isSourceFileKey = true), BlockConf(2, "homeHost", isSourceFileKey = true), BlockConf(3, "job"), BlockConf(4, "tag"))
  val jobStatusConf =
    Array(BlockConf(0, "block", jsonRoot = true), BlockConf(1, "igg", isSourceFileKey = true), BlockConf(2, "jobStatus"), BlockConf(3, "statusDate"), BlockConf(4, "tag"))
  val degreeConf =
    Array(BlockConf(0, "block", jsonRoot = true), BlockConf(1, "igg", isSourceFileKey = true), BlockConf(2, "degreeCode", isSourceFileKey = true), BlockConf(3, "degree"), BlockConf(4, "tag"))
  val degreeLevelConf =
    Array(BlockConf(0, "block", jsonRoot = true), BlockConf(1, "igg", isSourceFileKey = true), BlockConf(2, "degreeCode", isSourceFileKey = true), BlockConf(3, "degreeLevel"), BlockConf(4, "tag"))

  def extract(spark: SparkSession): DataFrame = {
    val file = sparkSession.sparkContext.textFile(filePath)
    val now = Calendar.getInstance()
    sparkSession
      .createDataFrame(file.map(lineBlockMatcher))
      .withColumn("executionId", lit(now.getTime.toString))
      .withColumn("miid", monotonically_increasing_id)
  }

  /**
    * Create a conf dataframe based on configuration file and join it with the input dataframe
    * in order to make the match sql
    *
    * @param line
    * @return
    */
  def lineBlockMatcher( line: String ): SourceView = {
    line.split("_")(0) match {
      case "IDENTIFICATION" =>
        buidlDataMapAndSourceKeys(idenfificationConf, line)
      case "NATIONALITY" =>
        buidlDataMapAndSourceKeys(nationalityConf, line)
      case "DEGREE" =>
        buidlDataMapAndSourceKeys(degreeConf, line)
      case "DEGREELEVEL" =>
        buidlDataMapAndSourceKeys(degreeLevelConf, line)
      case "CONTRACT" =>
        buidlDataMapAndSourceKeys(contractConf, line)
      case "JOB" =>
        buidlDataMapAndSourceKeys(jobConf, line)
      case "JOBSTATUS" =>
        buidlDataMapAndSourceKeys(jobStatusConf, line)
      //      case _ =>
      //        "{\"err\":\"line identifier not found\", \"line\":\""+ line +"\"}"
    }
  }

  def buidlDataMapAndSourceKeys( conf: Array[BlockConf], line: String ): SourceView = {
    val splitted = line.split("_")
    val jsonRoot =
      conf.filter(c => c.jsonRoot == true)
        .map(bc => splitted(bc.position))
        .mkString(start = "{\"", sep = "", end = "\"")
    val keys =
      conf.filter(c => c.isSourceFileKey == true)
        .map(bc => splitted(bc.position))
        .mkString(":")
    val jsonDataMap =
      conf
        //.filter(c => c.isSourceFileKey == false)
        .map(bc => (bc.name, splitted(bc.position)))
        .map(m => "\"" + m._1 + "\":" + "\"" + m._2 + "\"")
        .mkString(start = "{", sep = ",", end = "}")

    val dataMap =
      Map() ++ (
      conf.map(bc => (bc.name, splitted(bc.position)))
        )

    val jsonDataMapWithJsonRoot = jsonRoot + ": " + jsonDataMap + "}"

    SourceView(
      blockId = splitted(0),
      sourceKey = keys,
      dataMap = dataMap,
      jsonDataMap = jsonDataMapWithJsonRoot)
  }
}
