package com.yb.backend

import com.yb.backend.conf.ConfProxy._
import org.apache.spark
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, functions}

object Draft {

  def build(blockDfs: Map[String, DataFrame]): Map[String, DataFrame] ={
    val preDraftDfs = createDraftsByJoiningBlocks(blockDfs, loadJoinConf)
    preDraftDfs
      .map(x => NamedDataFrame(x._1, x._2))
      .map(preDraft =>
        {
          val relCols = relevantColumns(preDraft, preDraftSchemaMock)
          (preDraft.name -> selectRelevantAndRenameColumns(relCols, loadDraftTranslation("mock")))
        }
      ).toMap
  }

  private def createDraftsByJoiningBlocks( blockDfs: Map[String, DataFrame], joins: Array[JoinConf] ): Map[String, DataFrame] = {
    joins.map(j => (j.draft -> createDraft(blockDfs, j))).toMap
  }
  private def createDraft( blockDfs: Map[String, DataFrame], joinConf: JoinConf ): DataFrame = {
    val left: DataFrame = blockDfs.get(joinConf.leftTable).get
    recurseJoin(joinConf, left, blockDfs)
  }
  private def recurseJoin( jc: JoinConf, left: DataFrame, blockDfs: Map[String, DataFrame] ): DataFrame = {
    var joined = {
      if (jc.rightTable == null)
        left
      else
        joinWithConf(jc, left, blockDfs)
    }
    if (!jc.composition.isEmpty){
      for (joinConf <- jc.composition) {
        joined = recurseJoin(joinConf, joined, blockDfs)
      }
    }
    joined
  }
  private def joinWithConf( jc: JoinConf, left: DataFrame, blockDfs: Map[String, DataFrame] ): DataFrame = {
    val right: DataFrame = blockDfs.get(jc.rightTable).get
    left.join(right, left.col(jc.leftKey) === right.col(jc.rightKey), "left_outer")
  }

  private def preDraftSchemaMock: StructType ={
    loadPreDraftSchema(learningDraftConf)
  }
  private def relevantColumns( ddf: NamedDataFrame, preDraftSchema: StructType ): spark.sql.DataFrame = {
    ss.createDataFrame(ddf.df.rdd, preDraftSchema)
  }
  private def selectRelevantAndRenameColumns( preDraft: DataFrame, nameMapping: Map[String,String]): DataFrame ={
    preDraft.select(preDraft.columns.map(c => functions.col(c).as(nameMapping.getOrElse(c, c))): _*)
  }

  private def saveDraftAsparquet( ddf: NamedDataFrame, preDraft: DataFrame ): Unit = {
    preDraft
      .write.parquet("spark-warehouse/drafts/" + ddf.name + "-" + date + ".parquet")
  }



  private def testDraftCreation( preDraftDfs: Map[String, DataFrame] ): Unit = {
    preDraftDfs.foreach(ddf => {
      val ndf = NamedDataFrame(name = ddf._1, df = ddf._2)
      val preDraft = relevantColumns(ndf, preDraftSchemaMock)
      val nameMapping = loadDraftTranslation("mock")
      val draft = selectRelevantAndRenameColumns(ndf.df, nameMapping)
      saveDraftAsparquet(ndf, preDraft)
    })
  }
}
