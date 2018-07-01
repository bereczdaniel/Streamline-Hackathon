package eu.streamline.hackathon.flink.scala.job

import eu.streamline.hackathon.flink.scala.job.logic.InteractionCounterStream
import org.apache.flink.api.java.utils.ParameterTool


object FlinkScalaJob {
  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")
    val incrementalTopic = parameters.get("incTopic")
    val fulStateTopic = parameters.get("allTopic")
    val requestTopic = parameters.get("reqTopic")
    val port = parameters.get("port")

    InteractionCounterStream
      .pipeline(pathToGDELT, port, incrementalTopic, fulStateTopic, requestTopic)
  }

}
