package eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic

import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._

import scala.collection.mutable

class SimpleServerLogic(_init: Int => Parameter, _update: (Parameter, Parameter) => Parameter) extends ServerLogic {

  val state = new mutable.HashMap[Int, Parameter]()
  @transient lazy val init: ItemId => Parameter = _init
  @transient lazy val update: (Parameter, Parameter) => Parameter = _update


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
