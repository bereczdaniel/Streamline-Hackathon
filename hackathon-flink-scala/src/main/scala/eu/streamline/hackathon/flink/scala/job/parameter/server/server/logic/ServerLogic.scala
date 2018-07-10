package eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic

import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages.{Message, ParameterServerOutput}
import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.RecommendationSystemMessages.PullAnswer
import org.apache.flink.api.common.functions.RichMapFunction

abstract class ServerLogic extends RichMapFunction[Message, Either[ParameterServerOutput, PullAnswer]]
