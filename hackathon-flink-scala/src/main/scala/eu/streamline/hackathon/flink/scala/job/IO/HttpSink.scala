package eu.streamline.hackathon.flink.scala.job.IO

import com.google.gson.Gson
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient

class HttpSink extends RichSinkFunction[(String, Array[(String, Double)])]{

  lazy val url = "http://localhost:8081/"
  lazy val post = new HttpPost(url)
  def client = new DefaultHttpClient


  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    post.setHeader("Content-type", "application/json")
  }


  override def invoke(in: (String, Array[(String, Double)])): Unit = {

    post.setEntity(new StringEntity(new Gson().toJson(in)))


    // send the post request
    client.execute(post)

    client.close()
  }
}
