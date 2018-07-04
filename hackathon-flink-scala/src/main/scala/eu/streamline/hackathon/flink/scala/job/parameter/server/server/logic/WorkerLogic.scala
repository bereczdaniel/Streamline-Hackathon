package eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic

import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types.{Rating, ServerToWorker, WorkerOut, WorkerToServer}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction

abstract class WorkerLogic extends RichCoFlatMapFunction[Rating, ServerToWorker, Either[WorkerOut, WorkerToServer]]
