package eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic

import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction

abstract class WorkerLogic extends RichCoFlatMapFunction[PullAnswer, WorkerInput, Either[ParameterServerOutput, Message]]
