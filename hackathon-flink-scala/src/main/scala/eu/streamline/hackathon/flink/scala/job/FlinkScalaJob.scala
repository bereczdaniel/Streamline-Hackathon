package eu.streamline.hackathon.flink.scala.job

import eu.streamline.hackathon.flink.scala.job.IO.{GDELTSource, HttpSink}
import eu.streamline.hackathon.flink.scala.job.logic.InteractionCounterStream
import eu.streamline.hackathon.flink.scala.job.utils.Types.LightPostLoad
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object FlinkScalaJob {
  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val source = GDELTSource
      .read(env, pathToGDELT)
      .filter(event => event.actor1Code_countryCode != null && event.actor2Code_countryCode != null)

      InteractionCounterStream
          .generate(source)
          .addSink(new HttpSink[LightPostLoad])


    env.execute("Flink Scala GDELT Analyzer")
  }

}
