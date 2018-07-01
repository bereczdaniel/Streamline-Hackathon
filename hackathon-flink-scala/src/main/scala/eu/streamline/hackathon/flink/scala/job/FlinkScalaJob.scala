package eu.streamline.hackathon.flink.scala.job

import eu.streamline.hackathon.flink.scala.job.logic.RelationScoreStream
import org.apache.flink.api.java.utils.ParameterTool


object FlinkScalaJob {
  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")
    val incrementalTopic = parameters.get("incTopic")
    val fulStateTopic = parameters.get("allTopic")
    val requestTopic = parameters.get("reqTopic")
    val port = parameters.get("port")

    RelationScoreStream
      .pipeline(pathToGDELT, port, incrementalTopic, fulStateTopic, requestTopic, 8*math.pow(10, -9))
  }

}
