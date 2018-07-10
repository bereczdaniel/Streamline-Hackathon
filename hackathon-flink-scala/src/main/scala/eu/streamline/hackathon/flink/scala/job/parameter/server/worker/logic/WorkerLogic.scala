package eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic

import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages.{Message, ParameterServerOutput, WorkerInput}
import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.RecommendationSystemMessages.PullAnswer
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction

abstract class WorkerLogic extends RichCoFlatMapFunction[PullAnswer, WorkerInput, Either[ParameterServerOutput, Message]]
