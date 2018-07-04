package eu.streamline.hackathon.flink.scala.job.parameter.server.utils

object Types {

  case class Rating(userId: UserId, itemId: ItemId, rating: Double)

  sealed abstract class ParameterOutput(val id: Int, val parameter: Parameter){
    override def toString: String =
      id.toString + ":" + parameter.tail.foldLeft(parameter.head.toString)((acc, c) => acc + "," + c.toString)
  }
  case class WorkerOut(itemId: Int, params: Parameter) extends ParameterOutput(itemId, params)
  case class ServerOut(userId: Int, params: Parameter) extends ParameterOutput(userId, params)
  case class ServerToWorker(id: Int, source: Int, parameter: Parameter) {
    override def toString: String =
      id.toString + ":" + source.toString + ":" + parameter.tail.foldLeft(parameter.head.toString)((acc, c) => acc + "," + c.toString)
  }

  def serverToWorkerFromString(line: String): ServerToWorker = {
    val fields = line.split(":")
    ServerToWorker(fields(0).toInt, fields(1).toInt, fields(2).split(",").map(_.toDouble))
  }

  sealed abstract class WorkerToServer(val id: Int)
  case class Push(targetId: Int, parameter: Parameter) extends WorkerToServer(targetId){
    override def toString: String = {
      "Push:" + targetId.toString + ":" + parameter.tail.foldLeft(parameter.head.toString)((acc, c) => acc + "," + c.toString)
    }
  }
  case class Pull(targetId: Int, source: Int) extends WorkerToServer(targetId) {
    override def toString: String =
      "Pull:" + targetId.toString + ":" + source.toString
  }

  type Parameter = Array[Double]
  type UserId = Int
  type ItemId = Int


  /**
    * Exception to be thrown when a vector addition results in a NaN
    */
  class FactorIsNotANumberException extends Exception

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
