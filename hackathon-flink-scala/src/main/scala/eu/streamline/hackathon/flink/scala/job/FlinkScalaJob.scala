package eu.streamline.hackathon.flink.scala.job

import java.util.Date

import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.operations.GDELTInputFormat
import eu.streamline.hackathon.flink.scala.job.IO.HttpSink
import eu.streamline.hackathon.flink.scala.job.logic.{RelationMapState, RelationScoring, ScoreUpdate}
import eu.streamline.hackathon.flink.scala.job.utils.Types.{FullStatePostLoad, SimplifiedGDELT}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object FlinkScalaJob {
  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    implicit val typeInfo: TypeInformation[GDELTEvent] = createTypeInformation[GDELTEvent]
    implicit val dateInfo: TypeInformation[Date] = createTypeInformation[Date]

    val source = env
      .readFile[GDELTEvent](new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT)
      .setParallelism(1)

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
