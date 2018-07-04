package eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic

import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types.{ServerOut, ServerToWorker, WorkerToServer}
import org.apache.flink.api.common.functions.MapFunction

abstract class ServerLogic extends MapFunction[WorkerToServer, Either[ServerOut, ServerToWorker]]
