package eu.streamline.hackathon.flink.scala.job.logic

object RelationScoring {

  def simpleQuadTranslate(quadClass: Int, translate: Map[Int, Double]): Double = {
    translate(quadClass)
  }

  def normalizedQuadTranslate(quadClass: Int, translate: Map[Int, Int], normalizationParam: Int): Double = {
    translate(quadClass) / normalizationParam
  }


}
