package com.yb.backend

object AppBackEnd {

  /**
    * Source
    * {"block": "1", "value": [{"x":"A"}, {"x":"B"}]}
    * {"block": "2", "value": [{"y":"C","h":"D"}, {"y":"E","h":"F"}]}
    *
    * Explode on "value"
    * {"block": "1", "value": {"x":"A"}}
    * {"block": "1", "value": {"x":"B"}}
    * {"block": "2", "value": {"y":"C","h":"D"}}
    * {"block": "2", "value": {"y":"E","h":"F"}}
    *
    * Save to BlockView
    *
    * Join
    * {"x":"A", "y":"C", "h":"D"}
    * {"x":"B", "y":"E", "h":"F"}
    *
    * Filter and rename
    * {"name":"A", "age":"C", "gender":"D"}
    * {"name":"B", "age":"E", "gender":"F"}
    *
    * Functional controls (not yet implemented)
    *
    * Save to DraftView
    *
  */

  def main( args: Array[String] ): Unit = {
    //TrainingGenerator.process(4000000)
    val start = System.nanoTime()

    val rawView   = Raw.load
    val blockView = Block.build(rawView)
    val draftView = Draft.build(blockView)
    draftView.foreach(_._2.show)

    val end = System.nanoTime()
    println("duration "+ ((end - start)/ 1000000000.0))
  }

  /**
    * 18/05/28 20:42:08 WARN SparkSession$Builder: Using an existing SparkSession; some configuration may not take effect.
+------------+-----------+--------------------+-----------+---------------+-------------------+
|learningName|sessionName|       priceObjectId|priceAmount|sessionObjectId|   trainingObjectId|
+------------+-----------+--------------------+-----------+---------------+-------------------+
|    training|    2000134|learningName_2000134|    session|       10000670|sessionName_2000134|
|    training|    2000144|learningName_2000144|    session|       10000720|sessionName_2000144|
|    training|      20002|  learningName_20002|    session|         100010|  sessionName_20002|
|    training|    3333974|learningName_3333974|    session|       10001922|sessionName_3333974|
|    training|    2000456|learningName_2000456|    session|       10002280|sessionName_2000456|
|    training|     200048| learningName_200048|    session|        1000240| sessionName_200048|
|    training|     200056| learningName_200056|    session|        1000280| sessionName_200056|
|    training|    2000672|learningName_2000672|    session|       10003360|sessionName_2000672|
|    training|    2000768|learningName_2000768|    session|       10003840|sessionName_2000768|
|    training|    3335204|learningName_3335204|    session|       10005612|sessionName_3335204|
|    training|    2001508|learningName_2001508|    session|       10007540|sessionName_2001508|
|    training|    3335894|learningName_3335894|    session|       10007682|sessionName_3335894|
|    training|    2001762|learningName_2001762|    session|       10008810|sessionName_2001762|
|    training|    3336270|learningName_3336270|    session|       10008810|sessionName_3336270|
|    training|    3336426|learningName_3336426|    session|       10009278|sessionName_3336426|
|    training|    2001964|learningName_2001964|    session|       10009820|sessionName_2001964|
|    training|    2002124|learningName_2002124|    session|       10010620|sessionName_2002124|
|    training|    2002150|learningName_2002150|    session|       10010750|sessionName_2002150|
|    training|    2002162|learningName_2002162|    session|       10010810|sessionName_2002162|
|    training|    3337000|learningName_3337000|    session|       10011000|sessionName_3337000|
+------------+-----------+--------------------+-----------+---------------+-------------------+
only showing top 20 rows
for trainings-merged.json of 1,148 Go ( ~ 7 million lines)
duration 464.266627035 seconds
    */
}
