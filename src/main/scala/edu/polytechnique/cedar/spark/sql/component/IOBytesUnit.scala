package edu.polytechnique.cedar.spark.sql.component

import org.json4s.{JValue, JsonAST}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.render

case class IOBytesUnit(
    inputRead: Long,
    inputWritten: Long,
    shuffleRead: Long,
    shuffleWritten: Long
) extends MyUnit {

  def +(o: IOBytesUnit): IOBytesUnit =
    IOBytesUnit(
      inputRead + o.inputRead,
      inputWritten + o.inputWritten,
      shuffleRead + o.shuffleRead,
      shuffleWritten + o.shuffleWritten
    )

  val json: JsonAST.JObject =
    ("Total" -> (inputRead + inputWritten + shuffleRead + shuffleWritten)) ~
      ("Details" -> (
        ("IR" -> inputRead) ~
          ("IW" -> inputWritten) ~
          ("SR" -> shuffleRead) ~
          ("SW" -> shuffleWritten)
      ))
  override def toJson: JValue = render(json)
}

object IOBytesUnit {
  /** Neutral element for addition (all counters zero). */
  val zero: IOBytesUnit = IOBytesUnit(0L, 0L, 0L, 0L)
}

