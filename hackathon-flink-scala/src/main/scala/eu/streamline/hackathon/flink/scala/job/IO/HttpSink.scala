package eu.streamline.hackathon.flink.scala.job.IO

import com.google.gson.Gson
import eu.streamline.hackathon.flink.scala.job.utils.Types.{BasicPostLoad, FullStatePostLoad, LightPostLoad, StateRequest}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.slf4j.LoggerFactory

import scala.collection.mutable

class HttpSink[T <: BasicPostLoad](url: String, port: Int, endPointSimple: String, endPointRestore: String) extends RichSinkFunction[T]{

  private lazy val state: mutable.HashMap[(String, String), Double] = new mutable.HashMap[(String, String), Double]()

  private val log = LoggerFactory.getLogger(classOf[HttpSink[T]])
  lazy val simplePost = new HttpPost(url + ":" + port + "/" + endPointSimple)
  lazy val restorePost = new HttpPost(url + ":" + port + "/" + endPointRestore)
  def client = new DefaultHttpClient


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    simplePost.setHeader("Content-type", "application/json")
    restorePost.setHeader("Content-type", "application/json")
  }

  override def invoke(in: T): Unit = {
    in match {
      case StateRequest() =>
        restore()
      case _ =>
        update(in)
    }
    client.close()
  }



  def update(in: T): Unit = {
    updateState(in)
    simplePost.setEntity(new StringEntity(new Gson().toJson(in)))

    try {
      // send the post request
      client.execute(simplePost)
    }
    catch {
      case _: Throwable =>
        log.error("NoHttpResponseException: lost info of" + in)
    }
  }

  def restore(): Unit = {
    restorePost.setEntity(new StringEntity(new Gson().toJson(convertState())))
    try {
      // send the post request
      client.execute(restorePost)
    }
    catch {
      case _: Throwable =>
        log.error("NoHttpResponseException: unsuccessful restore")
    }
  }


  def updateState(in: T): Unit = {
    in match {
      case LightPostLoad(actor1, actor2, score) => state.update((actor1, actor2), score)
      case FullStatePostLoad(actor1, newState) =>
        newState.foreach(item => state.update((actor1, item._1), item._2))
    }
  }

  def convertState(): Array[(String, String, Double)] =
    (for( (k,v) <- state) yield (k._1, k._2, v)).toArray
}