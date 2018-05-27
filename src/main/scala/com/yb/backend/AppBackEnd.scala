package com.yb.backend

object AppBackEnd {

  def main( args: Array[String] ): Unit = {
    val rawView   = Raw.load
    val blockView = Block.build(rawView)
    val draftView = Draft.build(blockView)
    draftView.foreach(_._2.show)
  }
}
