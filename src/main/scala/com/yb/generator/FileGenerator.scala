package com.yb.generator

import java.time.LocalDate

import scala.util.Random

case class Identification(
                           block:String,
                           igg: String,
                           name: String,
                           firstName: String,
                           startDate: String,
                           tag: String
                         )
case class JobStatus(
                           block:String,
                           igg: String,
                           homeHost: String,
                           legalEntity: String,
                           startDate: String,
                           tag: String
                         )
case class Nationality(
                           block:String,
                           igg: String,
                           name: String,
                           firstName: String,
                           startDate: String,
                           tag: String
                         )
case class Contract(
                           block:String,
                           igg: String,
                           homeHost: String,
                           legalEntity: String,
                           contractType: String,
                           startDate: String,
                           tag: String
                         )
case class Degree(
                           block:String,
                           igg: String,
                           degreeCode: String,
                           degreeLib: String,
                           startDate: String,
                           tag: String
                         )
case class DegreeLevel(
                           block:String,
                           igg: String,
                           degreeLevelCode: String,
                           degreeLevelLib: String,
                           startDate: String,
                           tag: String
                         )

object FileGenerator {

  def gen(): Unit ={
    var igg = 1
    var homeHost = 0
    var legalEntity = "LegalEntity"
    var startDate = ""
    var name = ""
    var firstName = ""

    val ran = Random
    val from = LocalDate.of(1970, 1, 1)
    val to = LocalDate.of(2018, 1, 1)

    val identification: Seq[Identification] = Seq()

//    for(1 <- 10000){
//      igg = igg +1
//      homeHost = ran.nextBoolean().asInstanceOf[Int]
//      legalEntity = "LegalEntity" + igg
//      startDate = LocalDate.ofEpochDay(from.toEpochDay + ran.nextInt((to.toEpochDay - from.toEpochDay) toInt)).toString
//      name = "Name_"+igg
//      firstName
//
//      //identification ++ Identification(block = "IDENTIFICATION", igg = igg.toString, legalEntity = legalEntity, startDate = startDate, name = "")
//    }
  }


}
