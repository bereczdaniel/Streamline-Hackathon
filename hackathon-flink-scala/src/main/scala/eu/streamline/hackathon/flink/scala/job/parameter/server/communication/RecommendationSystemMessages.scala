package eu.streamline.hackathon.flink.scala.job.parameter.server.communication

import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages.{Message, ParameterServerOutput, WorkerInput}
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types.{ItemId, Parameter, TopK, UserId}
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Vector

object RecommendationSystemMessages {

  case class Rating(userId: UserId, itemId: ItemId, rating: Double) extends WorkerInput(itemId)
  case class EvaluationRequest(userId: UserId, itemId: ItemId, evaluationId: Long, rating: Double) extends WorkerInput(itemId)
  case class RecommendationRequest(userId: UserId, recommendationId: Int) extends WorkerInput(userId)
  case class NegativeExample(userId: UserId, itemId: ItemId, rating: Rating) extends WorkerInput(itemId)

  case class VectorModelOutput(id: Int, parameter: Vector) extends ParameterServerOutput {
    override def toString: String =
      id.toString + ":" + parameter.value.tail.foldLeft(parameter.value.head.toString)((acc, c) => acc + "," + c.toString)
  }
  case class RecommendationOutput(userId: UserId, topK: TopK) extends ParameterServerOutput {
    override def toString: String =
      userId.toString + ":" + topK.tail.foldLeft(topK.head.toString)((acc, c) => acc + "," + c.toString)
  }
  case class EvaluationOutput(itemId: ItemId, evaluationId: Long, topK: TopK) extends ParameterServerOutput

  case class PullAnswer(override val source: Int, override val destination: Int, parameter: Vector) extends Message(source, destination) {
    override def toString: String =
      destination.toString + ":" + source.toString + ":" + parameter.value.tail.foldLeft(parameter.value.head.toString)((acc, c) => acc + "," + c.toString)
  }

  def pullAnswerFromString(line: String): PullAnswer = {
    val fields = line.split(":")
    PullAnswer(fields(0).toInt, fields(1).toInt, Vector(fields(2).split(",").map(_.toDouble)))
  }

  case class Push(targetId: Int, parameter: Vector) extends Message(targetId, -1){
    override def toString: String = {
      "Push:" + targetId.toString + ":" + parameter.value.tail.foldLeft(parameter.value.head.toString)((acc, c) => acc + "," + c.toString)
    }
  }
  case class Pull(targetId: Int, workerSource: Int) extends Message(targetId, workerSource) {
    override def toString: String =
      "Pull:" + targetId.toString + ":" + source.toString
  }

}
