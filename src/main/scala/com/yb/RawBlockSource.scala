package com.yb

import java.util.Date

import com.yb.generator.TrainingGenerator
import net.liftweb.json.JObject
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object RawBlockSource {
  var count = 0
  val trainingFile = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\trainingCamel.json"
  val trainingBaseFile = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\trainingBaseCamel.json"
  val trainingNestedFile = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\trainings-merged.json"
//  val trainingNestedFile = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\training-nested.json"
  val joinConfPath = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\join.json"
  val learningDraftConf = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\learning-draft.json"
  val date = new Date().getTime
  val trainingGenericSchema =
    StructType(
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

  case class NamedDataFrame( endpointName: String, df: DataFrame )

  case class Draft( name: String, df: DataFrame )

  case class LearningDraft()

  /**
    * Read a json file where each line has a different schema
    * Each distinct schema is send to a dedicated dataframe
    * This way we can use spark native json support in a columnar way
    * Then in order to generate a super set of blocks allowing to create a Draft in
    * a 1 Source (n blocks) is enough to generate a Draft entity
    *
    * @param args
    */
  def main( args: Array[String] ): Unit = {
//    TrainingGenerator.process(10)
    TrainingGenerator.process(1000000)
    ss.sparkContext.setLogLevel("WARN")
    process
  }

  def ss = {
    SparkSession
      .builder()
      .appName("Simple Spark Application")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","file:///C:/Experiment/spark-2.0.0-bin-without-hadoop/spark-warehouse")
      .getOrCreate()
  }

  /**
    * Simulates a loaded input file configuration
    *
    * @return
    */
  def loadBlockConfiguration( ): Seq[BlockConf] = {
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

  def loadJoinConf( ): Array[JoinConf] = {
    val json = Source.fromFile(joinConfPath)
    net.liftweb.json.parse(json.mkString).extract[Array[JoinConf]]
  }
  def loadDraftMappingConf( path: String ): StructType = {
    val json = Source.fromFile(learningDraftConf)
    val conf = net.liftweb.json.parse(json.mkString)
      .extract[JObject]
      .values.keySet
      .toSeq
    fieldNames2StructType(conf)
  }

  /**
    * Generates a dynamic schema
    *
    * @param fields
    * @return
    */
  def fieldNames2StructType( fields: Seq[String] ): StructType = {
    val schema = ArrayBuffer[StructField]()
    schema.appendAll(
      fields.map(x => new StructField(x, StringType)).toList
    )
    StructType(schema)
  }

  def process( ): Unit = {
    /**
      * Load the file where each line as a different json schema as a map of prop/value, eg:
      * {"block": "1", "value": [{"x":"A"}, {"x":"B"}]}
      * {"block": "2", "value": [{"y":"C","h":"D"}, {"y":"E","h":"F"}]}
      */
    val fileDf = loadWithTrainingGenericSchema
    fileDf.show()
    /**
      * Explode on value property, eg:
      * {"block": "1", "value": {"x":"A"}}
      * {"block": "1", "value": {"x":"B"}}
      * {"block": "2", "value": {"y":"C","h":"D"}}
      * {"block": "2", "value": {"y":"E","h":"F"}}
      */
    val fileDfExploded = explodeValueArrayWithTrainingGenericSchema(fileDf)
    /**
      * Save each different "block" to a different parquet file
      */
    val blocksLocation: Seq[BlockLocation] = saveAsParquet(fileDfExploded)
    blocksLocation.foreach(println)
    val blockDfs: Map[String, DataFrame] = blocksLocation.map(
      bl => (bl.endpointName -> ss.read.parquet(bl.path))
    ).toMap
    blockDfs.foreach(bdf => bdf._2.show(false))
    /**
      * Load joiner configuration, one root element per Draft to generate
      */
    val joins = loadJoinConf
    /**
      * A draft needs several blocks materialised as dataframes to be joined to be generated
      * Each Draft will result in a DataFrame with its name
      */
    val draftDfs: Map[String, DataFrame] = joins.map(j => (j.draft -> createDraft(blockDfs, j))).toMap
    draftDfs.foreach(ddf => {
      val learningDraftSchema = loadDraftMappingConf(learningDraftConf) //oui ca mock dur
//      learningDraftSchema.printTreeString()
      println(ddf._1)
      ddf._2.show(false)
      ddf._2.printSchema()
      val preDraft = ss.createDataFrame(ddf._2.rdd, learningDraftSchema)
      preDraft.printSchema()
        preDraft
          .write.parquet("spark-warehouse/drafts/" + ddf._1 + "-" + date + ".parquet" )
    })



  }

  def createDraft( blockDfs: Map[String, DataFrame], joinConf: JoinConf ): DataFrame = {
    println("Draft root: " + joinConf.leftTable)
    val left: DataFrame = blockDfs.get(joinConf.leftTable).get
    recurseJoin(joinConf, left, blockDfs)
  }

  def recurseJoin( jc: JoinConf, left: DataFrame, blockDfs: Map[String, DataFrame] ): DataFrame = {
    count = count + 1
    var joined = {
      if (jc.rightTable == null)
        left
      else
        joinWithConf(jc, left, blockDfs)
    }
    if (!jc.composition.isEmpty){
      for (joinConf <- jc.composition) {
        println("--- recurse")
        joined.printSchema()
        joined = recurseJoin(joinConf, joined, blockDfs)
      }
    }
    joined
  }

  def joinWithConf( jc: JoinConf, left: DataFrame, blockDfs: Map[String, DataFrame] ): DataFrame = {
    //process current join
    println("*********************"+count+"****************************")
    println("   --> " + count + ": " + jc.rightTable + "." + jc.rightKey + " = " + jc.leftTable + "." + jc.leftKey)
//    left.printSchema()
    println(" -- LEFT -- ")
    left.show(false)
    val right: DataFrame = blockDfs.get(jc.rightTable).get
//    right.printSchema()
    println(" -- RIGHT -- ")
    right.show(false)
    println("*********************"+count+"****************************")
    left.join(right, left.col(jc.leftKey) === right.col(jc.rightKey), "left_outer")
  }

  def saveAsParquet( nestedExploded: DataFrame ): Seq[BlockLocation] = {
    loadBlockConfiguration
      .map(conf => (conf.endPoint, toParquetFile(conf, nestedExploded)))
      .map(t => BlockLocation(t._1, getParquetFileNameFromLocalDir(t._2)))
  }

  def testParquetFromLocal( fileNames: Seq[String] ): Unit = {
    fileNames
      .map(name => ss.read.parquet(name))
      .foreach(df => {
        df.show(false)
        df.printSchema
      }
      )
  }

  def loadWithTrainingGenericSchema( ): DataFrame = {
    ss.read.schema(trainingGenericSchema).json(trainingNestedFile)
  }

  def explodeValueArrayWithTrainingGenericSchema( nestedFile: DataFrame ): DataFrame = {
    import nestedFile.sparkSession.implicits._
    nestedFile
      .select($"source", explode($"value").as("value"))
      .select(to_json($"value").as("line"))
  }

  def getParquetFileNameFromLocalDir( dir: String ): String = {
    getListOfFiles(dir)
      .filter(f => f.getAbsolutePath.endsWith("parquet"))
      .mkString
  }
  def getListOfFiles( dir: String ): List[java.io.File] = {
    val d = new java.io.File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[java.io.File]()
    }
  }

  def toParquetFile( endpoint: BlockConf, file: DataFrame ): String = {
    import file.sparkSession.implicits._
    val fileName = "spark-warehouse/" + endpoint.endPoint + "-" + date
    //    println("----------- full !!!!!!!!")
    //    file.show(false)
    //    file.printSchema()
    //
    //    println("----------- filter on full !!!!!!!!")
    //
    //    file.filter($"line".contains(endpoint.endPoint)).show(false)
    //    endpoint.schema.printTreeString()
    val df = ss
      .read
      .schema(endpoint.schema)
      .json(file.filter($"line".contains(endpoint.endPoint)).as[String])
    //    println("----------- filtered !!!!!!!!")
    //    df.show(false)
    df
      .coalesce(1)
      .write
      .parquet(fileName)
    fileName
  }

}

