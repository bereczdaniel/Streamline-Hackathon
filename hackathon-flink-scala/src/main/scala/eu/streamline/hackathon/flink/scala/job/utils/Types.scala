package eu.streamline.hackathon.flink.scala.job.utils

object Types {

  sealed abstract class BasicInteraction(val actor1: String, val actor2: String, val score: Double)

  case class SimplifiedGDELT(actor1CountryCode: String, actor2CountryCode: String, quadClass: Int, quadScore: Int => Int)
    extends BasicInteraction(actor1CountryCode, actor2CountryCode, quadScore(quadClass))
}
