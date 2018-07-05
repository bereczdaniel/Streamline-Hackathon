package eu.streamline.hackathon.flink.scala.job.parameter.server.utils

object Types {

  sealed abstract class WorkerInput(val id: Int)
  case class Rating(userId: UserId, itemId: ItemId, rating: Double) extends WorkerInput(itemId)
  case class EvaluationRequest(userId: UserId, itemId: ItemId, recommendationId: Int, rating: Rating) extends WorkerInput(itemId)
  case class RecommendationRequest(userId: UserId, recommendationId: Int) extends WorkerInput(userId)
  case class NegativeExample(userId: UserId, itemId: ItemId, rating: Rating) extends WorkerInput(itemId)

  sealed trait ParameterServerOutput
  case class VectorModelOutput(id: Int, parameter: Parameter) extends ParameterServerOutput {
    override def toString: String =
      id.toString + ":" + parameter.tail.foldLeft(parameter.head.toString)((acc, c) => acc + "," + c.toString)
  }
  case class RecommendationOutput(userId: UserId, topK: TopK) extends ParameterServerOutput {
    override def toString: String =
      userId.toString + ":" + topK.tail.foldLeft(topK.head.toString)((acc, c) => acc + "," + c.toString)
  }


  sealed abstract class Message(val destination: Int, val source: Int)
  case class PullAnswer(targetId: Int, override val source: Int, parameter: Parameter) extends Message(targetId, source) {
    override def toString: String =
      targetId.toString + ":" + source.toString + ":" + parameter.tail.foldLeft(parameter.head.toString)((acc, c) => acc + "," + c.toString)
  }

  def pullAnswerFromString(line: String): PullAnswer = {
    val fields = line.split(":")
    PullAnswer(fields(0).toInt, fields(1).toInt, fields(2).split(",").map(_.toDouble))
  }

  case class Push(targetId: Int, parameter: Parameter) extends Message(targetId, -1){
    override def toString: String = {
      "Push:" + targetId.toString + ":" + parameter.tail.foldLeft(parameter.head.toString)((acc, c) => acc + "," + c.toString)
    }
  }
  case class Pull(targetId: Int, override val source: Int) extends Message(targetId, source) {
    override def toString: String =
      "Pull:" + targetId.toString + ":" + source.toString
  }

  type Parameter = Array[Double]
  type UserId = Int
  type ItemId = Int
  type TopK = Array[ItemId]


  /**
    * Exception to be thrown when a vector addition results in a NaN
    */
  class FactorIsNotANumberException extends Exception
  class NotSupportedWorkerInput extends Exception
  class NotSupportedMessage extends Exception
  class NotSupportedOutput extends Exception

  def vectorSum(u: Parameter, v: Parameter ): Array[Double] = {
    val n = u.length
    val res = new Array[Double](n)
    var i = 0
    while (i < n) {
      res(i) = u(i) + v(i)
      if (res(i).isNaN) {
        throw new FactorIsNotANumberException
      }
      i += 1
    }
    res
  }
}
