package com.yb.generator

import java.io.{File, PrintWriter}

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.write

import scala.io.Source

object TrainingGenerator {
  implicit val formats = DefaultFormats
  var MAX = 2000000
  val MOD_SESSION = 2
  val MOD_PRICE = 3
  case class Training( endPoint: String = "training", trainingObjectId: String, learningName: String )
  case class Session( endPoint: String = "session", sessionObjectId: String, sessionName: String, trainingFk: String )
  case class Price( endPoint: String = "price", priceObjectId: String, priceAmount: String, sessionFk: String )

  def main( args: Array[String] ): Unit = {
    process(MAX)
  }

  def process(max: Int): Unit ={
    MAX = max
    genStream()
    merge()
  }

  def lastTurn(max: Int, mod: Int): Int ={
    (max) - (max % mod)
  }

  def genStream( ) {
    val writerTrainings = new PrintWriter(new File("trainings-dummy.json"))
    val writerSessions = new PrintWriter(new File("sessions-dummy.json"))
    val writerPrices = new PrintWriter(new File("prices-dummy.json"))

    val template =
      """
        |{"source": "#", "autre": "other", "value": [
        |""".stripMargin
    writerTrainings.write(template.replace("#", "training"))
    writerSessions.write(template.replace("#", "session"))
    writerPrices.write(template.replace("#", "price"))

    val lastTraining = lastTurn(MAX, 1)
    val lastSession = lastTurn(MAX, MOD_SESSION)
    val lastPrice = lastTurn(MAX, MOD_PRICE)

    for (i <- 1 until MAX + 1) {

      var sepTraining: String = {
        if(i >= lastTraining)
          ""
        else
          ","
      }
      var sepSession: String = {
        if(i >= lastSession)
          ""
        else
          ","
      }
      var sepPrice: String = {
        if(i >= lastPrice)
          ""
        else
          ","
      }
//      println("turn: "+ i + ", max "+ MAX + " sepPrice: " + sepPrice + " - sepSession: "+sepSession)
//      println("turn: "+ i + ", max "+ MAX + " lastSession: " + lastSession + " - lastPrice: "+lastPrice)
//
//      println(" - write Training")
      writerTrainings.write(
        write(Training(trainingObjectId = "" + i, learningName = "learningName_" + i)))

      writerTrainings.write(sepTraining)

      if (i % MOD_SESSION == 0) {
//        println(" - write Session")
        writerSessions.write(
          write(Session(sessionObjectId = "" + i * 3, trainingFk = "" + i, sessionName = "sessionName_" + i)))

        writerSessions.write(",")

        writerSessions.write(
          write(Session(sessionObjectId = "" + i * 5, trainingFk = "" + i, sessionName = "sessionName_" + i)))

        writerSessions.write(sepSession)
      }
      if (i % MOD_PRICE == 0) {
//        println(" - write Price ")
        writerPrices.write(
          write(Price(priceObjectId = "" + i * 3, priceAmount = "" + i * 4, sessionFk = "" + i)))

        writerPrices.write(",")

        writerPrices.write(
          write(Price(priceObjectId = "" + i * 5, priceAmount = "" + i * 8, sessionFk = "" + i)))

        writerPrices.write(",")

        writerPrices.write(
          write(Price(priceObjectId = "" + i * 10, priceAmount = "" + i * 40, sessionFk = "" + i)))

        writerPrices.write(sepPrice)
      }
    }
    writerTrainings.write("]}")
    writerPrices.write("]}")
    writerSessions.write("]}")

    writerTrainings.close()
    writerPrices.close()
    writerSessions.close()
  }

  def merge( ): Unit = {
    val merger = new PrintWriter(new File("trainings-merged.json"))
//    Source.fromFile("trainings-dummy.json").foreach(c => merger.write(c))
//    Source.fromFile("sessions-dummy.json").foreach(c => merger.write(c))
//    Source.fromFile("prices-dummy.json").foreach(c => merger.write(c))
    partWrite(merger, "trainings-dummy.json")
    partWrite(merger, "sessions-dummy.json")
    partWrite(merger, "prices-dummy.json")
    merger.close
  }

  def partWrite(w: PrintWriter, path: String): Unit ={
    Source.fromFile(path).getLines().foreach(l => w.write(l))
    w.flush()
    w.write("\r\n")
  }
}
