package com.yb.helper

object FileHelper {

  def getParquetFileNameFromLocalDir(dir: String ): String = {
    getListOfFiles(dir)
      .filter(f => f.getAbsolutePath.endsWith("parquet"))
      .mkString
  }
  def getListOfFiles(dir: String ): List[java.io.File] = {
    val d = new java.io.File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[java.io.File]()
    }
  }
}
