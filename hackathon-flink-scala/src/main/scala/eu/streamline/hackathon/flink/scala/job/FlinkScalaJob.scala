package eu.streamline.hackathon.flink.scala.job

import eu.streamline.hackathon.flink.scala.job.IO.HttpSource
import eu.streamline.hackathon.flink.scala.job.IO.{GDELTSource, HttpSink}
import eu.streamline.hackathon.flink.scala.job.logic.InteractionCounterStream
import eu.streamline.hackathon.flink.scala.job.utils.Types
import eu.streamline.hackathon.flink.scala.job.utils.Types.StateRequest
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._


object FlinkScalaJob {
  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")
    val url = parameters.get("url")
    val port = parameters.get("port").toInt
    val endPointSimple = parameters.get("endPointSimple")
    val endPointRestore = parameters.get("endPointRestore")
    val endPointRestoreRquest = parameters.get("endPointRestoreRquest")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = GDELTSource
      .read(env, pathToGDELT)
      .filter(event => event.actor1Code_countryCode != null && event.actor2Code_countryCode != null)

    val fullStateRequest: DataStream[StateRequest] = env.addSource(new HttpSource(url, port, endPointRestoreRquest))

      InteractionCounterStream
          .generate(source)
          .connect(fullStateRequest)
          .map(l => l, r => r)
          .addSink(new HttpSink[Types.BasicPostLoad](url, port, endPointSimple, endPointRestore))

    env.execute("Flink Scala GDELT Analyzer")
  }

}
