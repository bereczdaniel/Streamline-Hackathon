package eu.streamline.hackathon.flink.scala.job.logic

object RelationScoring {

  def simpleQuadTranslate(quadClass: Int): Int = {
    quadClass match {
      case 1 => 1
      case 2 => 5
      case 3 => -1
      case 4 => -5
    }
  }
}
