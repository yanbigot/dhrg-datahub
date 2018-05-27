package com.yb.backend.conf

import java.util.Date

import net.liftweb.json.JObject
import org.apache.spark.sql.types.{DataTypes, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object ConfProxy {
  //for readibility purpose hold training file name, should be file, draftConf, genericSchema
  val trainingFile = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\trainings-merged.json"
  val joinConfPath = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\join.json"
  val learningDraftConf = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\learning-draft.json"
  val date = new Date().getTime
  val trainingGenericSchema = StructType(
      Seq(
        StructField("source", StringType, false),
        StructField("autre", StringType, true),
        StructField("value",
          DataTypes.createArrayType(
            DataTypes.createMapType(StringType, StringType, true)
            , true))
      )
    )

  implicit val formats = net.liftweb.json.DefaultFormats

  case class BlockConf( endPoint: String, schema: StructType )
  case class JoinConf( draft: String, rightTable: String, rightKey: String, leftTable: String, leftKey: String, composition: Array[JoinConf] )
  case class BlockLocation( endpointName: String, path: String )
  case class NamedDataFrame(name: String, df: DataFrame )
  case class Draft( name: String, df: DataFrame )
  case class DraftHolder()


  def ss = {
    val spark = SparkSession
      .builder()
      .appName("Simple Spark Application")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","file:///C:/Experiment/spark-2.0.0-bin-without-hadoop/spark-warehouse")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }

  def loadBlockConfigurationMock: Seq[BlockConf] = {
    Seq(
      BlockConf("training",
        fieldNames2StructType(
          Seq("endPoint", "trainingObjectId", "learningName"))),
      BlockConf("session",
        fieldNames2StructType(
          Seq("endPoint", "sessionObjectId", "sessionName", "trainingFk"))),
      BlockConf("price",
        fieldNames2StructType(
          Seq("endPoint", "priceObjectId", "priceAmount", "sessionFk")))
    )
  }

  def loadJoinConf: Array[JoinConf] = {
    val json = Source.fromFile(joinConfPath)
    net.liftweb.json.parse(json.mkString).extract[Array[JoinConf]]
  }

  def loadPreDraftSchema(path: String ): StructType = {
    val json = Source.fromFile(learningDraftConf)
    val conf = net.liftweb.json.parse(json.mkString)
      .extract[JObject]
      .values.keySet
      .toSeq
    fieldNames2StructType(conf)
  }

  def loadDraftTranslation(path: String): Map[String, String] ={
    val json = Source.fromFile(learningDraftConf)
    net.liftweb.json.parse(json.mkString)
      .values
      .asInstanceOf[Map[String, String]]
  }

  def fieldNames2StructType( fields: Seq[String] ): StructType = {
    val schema = ArrayBuffer[StructField]()
    schema.appendAll(
      fields.map(x => new StructField(x, StringType)).toList
    )
    StructType(schema)
  }

}
