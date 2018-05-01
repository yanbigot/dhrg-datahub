package com.yb.dfobj.functions

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}

/***
  * UDAF combining maps, overriding any duplicate key with "latest" value
  * @param keyType DataType of Map key
  * @param valueType DataType of Value key
  * @param merge function to merge values of identical keys
  * @tparam K key type
  * @tparam V value type
  */
class CombineMaps[K, V](keyType: DataType, valueType: DataType, merge: (V, V) => V) extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = new StructType().add("map", dataType)
  override def bufferSchema: StructType = inputSchema
  override def dataType: DataType = MapType(keyType, valueType)
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, Map[K, V]())

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val map1 = buffer.getAs[Map[K, V]](0)
    val map2 = input.getAs[Map[K, V]](0)
    val result = map1 ++ map2.map { case (k,v) => k -> map1.get(k).map(merge(v, _)).getOrElse(v) }
    buffer.update(0, result)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = update(buffer1, buffer2)

  override def evaluate(buffer: Row): Any = buffer.getAs[Map[String, Int]](0)
}

  object Example {
    def main( args: Array[String] ): Unit = {
      import org.apache.spark.sql.functions._

      val sc: SparkContext = new SparkContext("local", "test")
      val sqlContext = new SQLContext(sc)

      import sqlContext.implicits._
      val input = sc.parallelize(Seq(
        (1, Map("John" -> 12.5, "Alice" -> 5.5)),
        (1, Map("Jim" -> 16.5, "Alice" -> 4.0)),
        (2, Map("John" -> 13.5, "Jim" -> 2.5))
      )).toDF("id", "scoreMap")

      // instantiate a CombineMaps with the relevant types:
      val combineMaps = new CombineMaps[String, Double](StringType, DoubleType, _ + _)

      // groupBy and aggregate
      val result = input.groupBy("id").agg(combineMaps(col("scoreMap")))

      result.show(truncate = false)
    }
  }
