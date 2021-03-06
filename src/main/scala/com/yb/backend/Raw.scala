package com.yb.backend

import com.yb.backend.conf.ConfProxy._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{explode, to_json}
/**
  * Some file dependent pre-processings might be needed and not that generic
  * Here training as example
  */
object Raw {

  def load(): DataFrame ={
    val file = loadWithGenericSchema
    file.printSchema()
    file.show()
    explodeThenJson(file)
  }

  /**
    * Training file specific, perf optimisation, could be used
    * without schema...
    * @return
    */
  private def loadWithGenericSchema: DataFrame = {
    ss.read
      .schema(trainingGenericSchema)
      .json(trainingFile)
  }
  private def explodeThenJson(nestedFile: DataFrame ): DataFrame = {
    import nestedFile.sparkSession.implicits._
    nestedFile
      .select($"source", explode($"value").as("value"))
      .select(to_json($"value").as("line"))
  }
}
