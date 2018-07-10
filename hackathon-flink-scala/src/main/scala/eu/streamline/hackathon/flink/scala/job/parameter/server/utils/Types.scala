package eu.streamline.hackathon.flink.scala.job.parameter.server.utils

object Types {
  type Parameter = Array[Double]
  type UserId = Int
  type ItemId = Int
  type TopK = List[(ItemId, Double)]
}

