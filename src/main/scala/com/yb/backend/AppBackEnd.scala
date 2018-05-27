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
    val rawView   = Raw.load
    val blockView = Block.build(rawView)
    val draftView = Draft.build(blockView)
    draftView.foreach(_._2.show)
  }
}
