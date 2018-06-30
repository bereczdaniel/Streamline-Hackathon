package eu.streamline.hackathon.flink.scala.job.utils

object Types {

  sealed abstract class BasicInteraction(val actor1: String, val actor2: String, val score: Double)

  case class SimplifiedGDELT(actor1CountryCode: String, actor2CountryCode: String, quadClass: Int, quadScore: Int => Int)
    extends BasicInteraction(actor1CountryCode, actor2CountryCode, quadScore(quadClass))

  sealed trait BasicPostLoad
  case class LightPostLoad(actor1: String, actor2: String, score: Double) extends BasicPostLoad
  case class FullStatePostLoad(actor1: String, state: Array[(String, Double)]) extends BasicPostLoad
}
