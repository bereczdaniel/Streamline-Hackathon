package eu.streamline.hackathon.flink.scala.job.parameter.server.IO

import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages.ParameterServerOutput
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

trait ParameterServerOutputSink extends RichSinkFunction[ParameterServerOutput]
