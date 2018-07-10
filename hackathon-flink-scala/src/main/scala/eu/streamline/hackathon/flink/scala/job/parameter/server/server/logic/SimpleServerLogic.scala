package eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic

import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages.{Message, NotSupportedMessage, ParameterServerOutput}
import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.RecommendationSystemMessages.{Pull, PullAnswer, Push, VectorModelOutput}
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types.ItemId
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Vector

import scala.collection.mutable

class SimpleServerLogic(_init: Int => Vector, _update: (Vector, Vector) => Vector) extends ServerLogic {

  val state = new mutable.HashMap[Int, Vector]()
  @transient lazy val init: ItemId => Vector = _init
  @transient lazy val update: (Vector, Vector) => Vector = _update


  override def map(value: Message): Either[ParameterServerOutput, PullAnswer] = {
    value match {
      case Pull(targetId, workerSource) =>
        Right(PullAnswer(targetId, workerSource, state.getOrElseUpdate(targetId, init(targetId))))
      case Push(id, parameter) =>
        state.update(id, update(parameter, state.getOrElseUpdate(id, init(id))))
        Left(VectorModelOutput(id, state(id)))
      case _ =>
        throw new NotSupportedMessage
    }
  }
}
