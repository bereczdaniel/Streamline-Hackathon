package eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic

import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._
import eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic.ServerLogic

import scala.collection.mutable

class SimpleServerLogic(_init: Int => Parameter, _update: (Parameter, Parameter) => Parameter) extends ServerLogic {

  val state = new mutable.HashMap[Int, Parameter]()
  @transient lazy val init: ItemId => Parameter = _init
  @transient lazy val update: (Parameter, Parameter) => Parameter = _update


  override def map(value: Types.WorkerToServer): Either[Types.ServerOut, Types.ServerToWorker] = {
    value match {
      case Pull(id, source) =>
        Right(ServerToWorker(id, source, state.getOrElseUpdate(id, init(id))))
      case Push(id, parameter) =>
        state.update(id, update(parameter, state.getOrElseUpdate(id, init(id))))
        Left(ServerOut(id, state(id)))
    }
  }
}
