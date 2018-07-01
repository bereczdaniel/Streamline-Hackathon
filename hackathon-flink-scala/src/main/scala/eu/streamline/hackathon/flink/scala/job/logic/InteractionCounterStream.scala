package eu.streamline.hackathon.flink.scala.job.logic

import java.util.Properties

import com.google.gson.Gson
import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.scala.job.IO.GDELTSource
import eu.streamline.hackathon.flink.scala.job.utils.Types.{CountryCounter, LightPostLoad, StateRequest}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector

import scala.collection.mutable

class InteractionCounterStream {

}


object InteractionCounterStream {

  def pipeline(pathToGDELT: String, port: String, incrementalTopic: String, fullStateTopic: String, stateReqTopic: String): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val source = GDELTSource
      .read(env, pathToGDELT)
      .filter(event => event.actor1Code_countryCode != null && event.actor2Code_countryCode != null)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:"+port)
    properties.setProperty("group.id", "hackathon")

    val fullStateRequest = env
      .addSource(new FlinkKafkaConsumer011[String](stateReqTopic, new serialization.SimpleStringSchema(), properties))
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
      .map(_.toString)



    fullState
      .addSink(new FlinkKafkaProducer011[String]("localhost:"+port, fullStateTopic, new SimpleStringSchema()))

    incremental
      .addSink(new FlinkKafkaProducer011[String]("localhost:"+port, incrementalTopic, new SimpleStringSchema()))

    env.execute("Flink Scala GDELT Analyzer")
  }

  def generate(src: DataStream[GDELTEvent], stateRequest: DataStream[StateRequest]): DataStream[Either[LightPostLoad, Array[(String, String, Double)]]] = {
    src
      .map(event => {
        val (a1,a2) = {
          if(event.actor1Code_countryCode > event.actor2Code_countryCode)
            (event.actor1Code_countryCode, event.actor2Code_countryCode)
          else
            (event.actor2Code_countryCode, event.actor1Code_countryCode)
        }
        CountryCounter(a1, a2)
      })
      .keyBy(_.actor1)
      .connect(stateRequest.broadcast)
      .flatMap(new RichCoFlatMapFunction[CountryCounter, StateRequest, Either[LightPostLoad, Array[(String, String, Double)]]] {
        private lazy val state: mutable.HashMap[(String, String), Double] = new mutable.HashMap[(String, String), Double]()

        override def flatMap1(value: CountryCounter, out: Collector[Either[LightPostLoad, Array[(String, String, Double)]]]): Unit = {
          val newScore = state.getOrElseUpdate((value.actor1, value.actor2), 0) + 1
          out.collect(Left(LightPostLoad(value.actor1, value.actor2, newScore)))
          state.update((value.actor1, value.actor2), newScore)
        }

        override def flatMap2(value: StateRequest, out: Collector[Either[LightPostLoad, Array[(String, String, Double)]]]): Unit = {
          out.collect(Right((for( (k,v) <- state) yield (k._1, k._2, v)).toArray))
        }
      })
  }

  def windowedUpdate(src: DataStream[GDELTEvent]): DataStream[CountryCounter] = {
    src
      .map(event => {
      val (a1,a2) = {
        if(event.actor1Code_countryCode > event.actor2Code_countryCode)
          (event.actor1Code_countryCode, event.actor2Code_countryCode)
        else
          (event.actor2Code_countryCode, event.actor1Code_countryCode)
      }
      CountryCounter(a1, a2, ts ={
        try{
          event.dateAdded.getTime
        }
        catch {
          case _: Throwable => -1L
        }
      })
    })
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[CountryCounter] {
        private var cwm = 0L
        override def getCurrentWatermark: Watermark =
          new Watermark(cwm)

        override def extractTimestamp(element: CountryCounter, previousElementTimestamp: Long): Long = {
          cwm = {
            element.getEventTime match {
              case -1 => previousElementTimestamp
              case i: Long => i
            }
          }
          cwm
        }
      })
      .keyBy(x => (x.actor1, x.actor2))
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .process(new ProcessWindowFunction[CountryCounter, CountryCounter, (String, String), TimeWindow] {
        override def process(key: (String, String), context: Context, elements: Iterable[CountryCounter], out: Collector[CountryCounter]): Unit = {

          val score = elements.map(_.numInteractions).sum
          out.collect(CountryCounter(key._1, key._2, score))
        }
      })

    null
  }
}