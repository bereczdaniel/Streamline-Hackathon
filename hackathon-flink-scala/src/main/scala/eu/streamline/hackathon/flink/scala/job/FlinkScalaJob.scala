package eu.streamline.hackathon.flink.scala.job

import eu.streamline.hackathon.flink.scala.job.IO.{GDELTSource, HttpSink}
import eu.streamline.hackathon.flink.scala.job.logic.state.InteractionCounterState
import eu.streamline.hackathon.flink.scala.job.utils.Types.{CountryCounter, LightPostLoad}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

object FlinkScalaJob {
  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val source = GDELTSource
      .read(env, pathToGDELT)
      .filter(event => event.actor1Code_countryCode != null && event.actor2Code_countryCode != null)
      .map(event => CountryCounter(event.actor1Code_countryCode, event.actor2Code_countryCode))

    val ac1 = source
        .keyBy(_.actor1)
        .mapWithState((in: CountryCounter, state: Option[mutable.HashMap[String, Int]]) => {
          state match {
            case None =>
              val hm = new mutable.HashMap[String, Int]()
              hm.update(in.actor2, 1)
              (LightPostLoad(in.actor1, in.actor2, 1), Some(hm))
            case Some(cur) =>
              val counter = cur.getOrElseUpdate(in.actor2, 1)
              cur.update(in.actor2, counter + 1)
              (LightPostLoad(in.actor1, in.actor2, cur(in.actor2)), Some(cur))
          }
        })

    val ac2  = source
      .keyBy(_.actor2)
      .mapWithState((in: CountryCounter, state: Option[mutable.HashMap[String, Int]]) => {
        state match {
          case None =>
            val hm = new mutable.HashMap[String, Int]()
            hm.update(in.actor1, 1)
            (LightPostLoad(in.actor2, in.actor1, 1), Some(hm))
          case Some(cur) =>
            val counter = cur.getOrElseUpdate(in.actor1, 1)
            cur.update(in.actor1, counter + 1)
            (LightPostLoad(in.actor2, in.actor1, cur(in.actor1)), Some(cur))
        }
      })

    ac1
        .connect(ac2)
        .map(x => x, y => y)
        .addSink(new HttpSink[LightPostLoad])


    env.execute("Flink Scala GDELT Analyzer")
  }

}
