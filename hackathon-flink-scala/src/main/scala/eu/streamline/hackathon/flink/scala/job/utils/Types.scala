package eu.streamline.hackathon.flink.scala.job.utils

object Types {

  sealed abstract class BasicInteraction(val actor1: String, val actor2: String, val score: Double)

  case class CountryBasedInteraction(actor1CountryCode: String, actor2CountryCode: String,
                                     quadClass: Int, translate: Map[Int, Double], quadScore: (Int, Map[Int, Double]) => Double)
    extends BasicInteraction(actor1CountryCode, actor2CountryCode, quadScore(quadClass, translate))

  case class CountryCounter(actor1: String, actor2: String)

  sealed trait BasicPostLoad
  case class LightPostLoad(actor1: String, actor2: String, score: Double) extends BasicPostLoad
  case class FullStatePostLoad(actor1: String, state: Array[(String, Double)]) extends BasicPostLoad
}
