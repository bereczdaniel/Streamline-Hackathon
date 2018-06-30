package eu.streamline.hackathon.flink.scala.job.logic.relation.scores

object ScoreUpdate {

  def simpleSum(currentScore: Double, incomingScore: Double): Double =
    currentScore + incomingScore

  def weightedSum(currentWeight: Double)(currentScore: Double, incomingScore: Double): Double =
    currentScore*currentWeight + incomingScore*(1-currentWeight)
}
