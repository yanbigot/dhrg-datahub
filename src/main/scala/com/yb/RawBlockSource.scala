package com.yb



import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

object RawBlockSource {
  val trainingFile = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\trainingCamel.json"
  val trainingBaseFile = "D:\\NIFI_WORKSPACE\\dhrg-datahub\\src\\main\\resources\\trainingBaseCamel.json"

  def sparkSession = {
    val spark = SparkSession
      .builder()
      .appName("Simple Spark Application")
      .master("local[*]")
      .getOrCreate()
    spark
  }

  case class Conf(endPoint: String, schema: StructType)

  /**
    * Simulates a loaded input file configuration
    * @return
    */
  def buildConf(): Seq[Conf] ={
    Seq(
      Conf("training_base",
        fieldNames2StructType(
          Seq("endPoint","objectId", "learningName"))),
      Conf("training_session",
        fieldNames2StructType(
          Seq("endPoint","sessionObjectId", "sessionName", "trainFk"))),
      Conf("training_price",
        fieldNames2StructType(
          Seq("endPoint","priceObjectId", "priceAmount", "tSessFk")))
    )
  }

  /**
    * Generates a dynamic schema
    * @param fields
    * @return
    */
  def fieldNames2StructType(fields: Seq[String]): StructType ={
    val schema = ArrayBuffer[StructField]()
    schema.appendAll(
      fields.map(x => new StructField(x, StringType)).toList
    )
    StructType(schema)
  }

  /**
    * Read a json file where each line has a different schema
    * Each distinct schema is send to a dedicated dataframe
    * This way we can use spark native json support in a columnar way
    * Then in order to generate a super set of blocks allowing to create a Draft in
    * a 1 Source (n blocks) is enough to generate a Draft entity
    * @param args
    */
  def main( args: Array[String] ): Unit = {
    val spark: SparkSession = sparkSession
    import spark.implicits._
    // read line by line
    val file: Dataset[String] = spark.read.textFile(trainingFile)
    //iterate on file as dataframe : each block is a dedicated dataframe
    var dfs:Map[String,DataFrame] = Map()
    for(c  <- buildConf){
      dfs += (c.endPoint -> createBlockDf(c, spark, file))
    }
    dfs.foreach(x => x._2.printSchema())
    dfs.foreach(x => x._2.show(false))

    //defined set of join condition on a source view perspective
    val joinCondition = Seq("training_base#objectId","training_session#trainFk" )
    val split_A = joinCondition(0).split("#")
    val split_B = joinCondition(1).split("#")
    val df_A_Conf = (split_A(0), split_A(1))
    val df_B_Conf = (split_B(0), split_B(1))
    //simulate iteration on df list
    val dfa = dfs.get(df_A_Conf._1).get
    val dfb = dfs.get(df_B_Conf._1).get
    //simulate iterate on join conditions => generates a draft
    val draft = dfa.join(dfb, dfa.col(df_A_Conf._2) === dfb.col(df_B_Conf._2), "left")
    draft.show(false)
  }

  /**
    * Creates dataframe by filter based on endpoint name
    * @param endpoint
    * @param spark
    * @param file
    * @return
    */
  def createBlockDf(endpoint: Conf, spark: SparkSession, file: Dataset[String]): DataFrame = {
    import spark.implicits._
    spark.read.schema(endpoint.schema).json(file.filter(l => l.contains(endpoint.endPoint)))
  }

}

