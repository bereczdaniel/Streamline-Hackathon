package eu.streamline.hackathon.flink.scala.job

import java.util.Properties

import com.google.gson.Gson
import eu.streamline.hackathon.flink.scala.job.IO.GDELTSource
import eu.streamline.hackathon.flink.scala.job.logic.InteractionCounterStream
import eu.streamline.hackathon.flink.scala.job.utils.Types.{LightPostLoad, StateRequest}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector
import org.apache.http.entity.StringEntity


object FlinkScalaJob {
  def main(args: Array[String]): Unit = {

    val parameters = ParameterTool.fromArgs(args)
    val pathToGDELT = parameters.get("path")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = GDELTSource
      .read(env, pathToGDELT)
      .filter(event => event.actor1Code_countryCode != null && event.actor2Code_countryCode != null)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "stateRequest")

    val fullStateRequest = env
      .addSource(new FlinkKafkaConsumer011[String]("stateRequest", new serialization.SimpleStringSchema(), properties))
      .filter(event => event == "yes")
      .map(_=> StateRequest())

    val common = InteractionCounterStream
      .generate(source, fullStateRequest)

    val fullState =  common
        .flatMap(new RichFlatMapFunction[Either[LightPostLoad, Array[(String, String, Double)]], String] {
          override def flatMap(value: Either[LightPostLoad, Array[(String, String, Double)]], out: Collector[String]): Unit = {
            value match {
              case Left(_) =>
              case Right(update) => out.collect(new Gson().toJson(update))
            }
          }
        })

    val incremental = common
      .flatMap(new RichFlatMapFunction[Either[LightPostLoad, Array[(String, String, Double)]], LightPostLoad] {
        override def flatMap(value: Either[LightPostLoad, Array[(String, String, Double)]], out: Collector[LightPostLoad]): Unit = {
          value match {
            case Left(update) =>
              Thread.sleep(10)
              out.collect(update)
            case Right(_) =>
          }
        }
      })
        /*.keyBy(v => (v.actor1, v.actor2))
        .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
        .maxBy("score")*/
        //.reduce((a,b) => LightPostLoad(a.actor1, a.actor2, a.score + b.score))
        .map(_.toString)



    fullState
        .addSink(new FlinkKafkaProducer011[String]("localhost:9092", "fullStateUpdate", new SimpleStringSchema()))

    incremental
        .addSink(new FlinkKafkaProducer011[String]("localhost:9092", "incrementalUpdate", new SimpleStringSchema()))

    env.execute("Flink Scala GDELT Analyzer")
  }

}
