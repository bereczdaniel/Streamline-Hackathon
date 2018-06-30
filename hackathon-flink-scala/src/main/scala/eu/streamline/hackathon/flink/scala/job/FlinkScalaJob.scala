package eu.streamline.hackathon.flink.scala.job

import java.util.Date

import com.google.gson.Gson
import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.operations.GDELTInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient

import scala.collection.mutable

object FlinkScalaJob {

  case class SimplifiedGDELT(actor1: String, actor2: String, quadClass: Int)

  def translateQuadClass(in: Int): Int = {
    in match {
      case 1 => 1
      case 2 => 5
      case 3 => -1
      case 4 => -5
      case _ => 0
    }
  }
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
        .filter(event => event.actor1Code_countryCode != null && event.actor1Code_countryCode != null)
        .map(event => SimplifiedGDELT(event.actor1Code_countryCode, event.actor2Code_countryCode, translateQuadClass(event.quadClass)))
        .keyBy(_.actor1)
        .mapWithState((event: SimplifiedGDELT, realations: Option[mutable.HashMap[String, Int]]) => {
          realations match {
            case None =>
              val hm = new mutable.HashMap[String, Int]()
              hm(event.actor2) = event.quadClass
              ((event.actor1, hm), Some(hm))
            case Some(state) =>
              val cur = state.getOrElse(event.actor2, 0)
              state.update(event.actor2, cur + event.quadClass)
              ((event.actor1, state), Some(state))
          }
        })
        .addSink(new RichSinkFunction[(String, mutable.HashMap[String, Int])] {
          lazy val url = "http://localhost:8081/"
          lazy val post = new HttpPost(url)
          def client = new DefaultHttpClient


          override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            post.setHeader("Content-type", "application/json")
          }


          override def invoke(in: (String, mutable.HashMap[String, Int])): Unit = {
            val temp = (for((k,v) <-  in._2) yield (k,v)).toArray
            post.setEntity(new StringEntity(new Gson().toJson((in._1, temp))))


            // send the post request
            client.execute(post)

            client.close()
          }
        })
    env.execute("Flink Scala GDELT Analyzer")

  }

}
