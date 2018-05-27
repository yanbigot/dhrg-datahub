package com.yb.backend

//import com.yb.backend.conf.ConfProxy._
import com.yb.RawBlockSource.BlockLocation
import com.yb.helper.FileHelper._
import com.yb.backend.conf.ConfProxy._
import org.apache.spark
import org.apache.spark.sql.DataFrame

object Block {

  def build(raw: DataFrame): Map[String, DataFrame] ={
    val blockConf = loadBlockConfigurationMock
    val blocks = createBlocksByFilter(raw, blockConf)

    saveBlocksAsParquet(blocks)

    val blocksLocation = retrieveParquetFileNameOnWindows(blocks)//because local, and parquet fileName is the directory, not the parquet file !
    val blockDfs = reloadBlocks(blocksLocation)
    blockDfs
  }

  private def createBlocksByFilter( raw: DataFrame, blockConf: Seq[BlockConf] ): Seq[NamedDataFrame] = {
    blockConf
      .map(conf => NamedDataFrame(conf.endPoint, createBlockByFilter(conf, raw)))
  }
  private def createBlockByFilter( endpoint: BlockConf, file: DataFrame ): DataFrame = {
    import file.sparkSession.implicits._
    ss
      .read
      .schema(endpoint.schema)
      .json(file.filter($"line".contains(endpoint.endPoint)).as[String])
  }

  private def saveBlocksAsParquet( blocks: Seq[NamedDataFrame] ): Unit = {
    blocks.foreach(ndf => saveAsParquet(ndf.df, withFileName(ndf.name)))
  }
  private def saveAsParquet(block: DataFrame, fileName: String): Unit = {
    block.coalesce(1).write.parquet(fileName)
  }
  private def withFileName(blockName: String): String ={
    "spark-warehouse/" + blockName + "-" + date
  }

  private def retrieveParquetFileNameOnWindows( blocks: Seq[NamedDataFrame] ): Seq[BlockLocation] = {
    blocks
      .map(ndf => BlockLocation(ndf.name, getParquetFileNameFromLocalDir(withFileName(ndf.name))))
  }
  private def reloadBlocks( blocksLocation: Seq[BlockLocation] ): Map[String, spark.sql.DataFrame] = {
    blocksLocation.map(
      bl => (bl.endpointName -> ss.read.parquet(bl.path))
    ).toMap
  }
}
