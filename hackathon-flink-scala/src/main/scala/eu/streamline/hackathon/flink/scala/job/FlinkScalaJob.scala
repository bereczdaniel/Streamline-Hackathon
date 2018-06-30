package eu.streamline.hackathon.flink.scala.job

import eu.streamline.hackathon.flink.scala.job.IO.{GDELTSource, HttpSink}
import eu.streamline.hackathon.flink.scala.job.logic.{RelationMapState, RelationScoring, ScoreUpdate}
import eu.streamline.hackathon.flink.scala.job.utils.Types.{FullStatePostLoad, SimplifiedGDELT}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object FlinkScalaJob {
  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val source = GDELTSource.read(env, pathToGDELT)

    source
        .filter(event => event.actor1Code_countryCode != null && event.actor2Code_countryCode != null)
        .map(event => SimplifiedGDELT(
          event.actor1Code_countryCode, event.actor2Code_countryCode,
          event.quadClass, Map(1 -> 1, 2 -> 5, 3 -> -1, 4 -> -5), RelationScoring.simpleQuadTranslate))
        .keyBy(_.actor1CountryCode)
        .map(new RelationMapState[SimplifiedGDELT](ScoreUpdate.weightedSum(0.9)))
        .addSink(new HttpSink[FullStatePostLoad]())


    env.execute("Flink Scala GDELT Analyzer")
  }

}
