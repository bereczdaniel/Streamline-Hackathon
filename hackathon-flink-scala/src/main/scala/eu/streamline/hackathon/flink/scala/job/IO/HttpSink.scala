package eu.streamline.hackathon.flink.scala.job.IO

import com.google.gson.Gson
import eu.streamline.hackathon.flink.scala.job.utils.Types.BasicPostLoad
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.http.NoHttpResponseException
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.slf4j.LoggerFactory

class HttpSink[T <: BasicPostLoad] extends RichSinkFunction[T]{

  private val log = LoggerFactory.getLogger(classOf[HttpSink[T]])
  lazy val url = "http://localhost:8081/"
  lazy val post = new HttpPost(url)
  def client = new DefaultHttpClient


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    post.setHeader("Content-type", "application/json")
  }


  override def invoke(in: T): Unit = {

    post.setEntity(new StringEntity(new Gson().toJson(in)))


    try {
      // send the post request
      client.execute(post)
    }
    catch {
      case NoHttpResponseException => log.error("NoHttpResponseException: lost info of" + in)
    }

    client.close()
  }
}
