package eu.streamline.hackathon.flink.scala.job.utils

object Types {

  trait EventWithTimestamp {
    def getEventTime: Long
  }
  
  sealed abstract class BasicInteraction(val actor1: String, val actor2: String, val score: Double)

  case class CountryBasedInteraction(actor1CountryCode: String, actor2CountryCode: String,
                                     quadClass: Int, ts: Long, translate: Map[Int, Double], quadScore: (Int, Map[Int, Double]) => Double)
    extends BasicInteraction(actor1CountryCode, actor2CountryCode, quadScore(quadClass, translate))

  case class CountryCounter(actor1CountryCode: String, actor2CountryCode: String, numInteractions: Int = 1, ts: Long = 0L) 
    extends BasicInteraction(actor1CountryCode, actor2CountryCode, numInteractions) with EventWithTimestamp {
    override def getEventTime: Long = ts
  }

  
  
  sealed trait BasicPostLoad extends Product with Serializable
  case class LightPostLoad(actor1: String, actor2: String, score: Double) extends BasicPostLoad
  case class StateRequest() extends BasicPostLoad
  
}
