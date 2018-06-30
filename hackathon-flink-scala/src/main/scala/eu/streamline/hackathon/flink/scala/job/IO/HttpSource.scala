package eu.streamline.hackathon.flink.scala.job.IO

import eu.streamline.hackathon.flink.scala.job.utils.Types.StateRequest
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils

class HttpSource(url: String, port: Int, endPoint: String) extends SourceFunction[StateRequest]{

  lazy val get = new HttpGet(url + ":" + port + "/" + endPoint)
  def client = new DefaultHttpClient



  override def run(ctx: SourceFunction.SourceContext[StateRequest]): Unit = {
    while(true){
      val response = client.execute(get)
      val answer = EntityUtils.toString(response.getEntity)
      answer match {
        case "yes" =>
          ctx.collect(StateRequest())
          Thread.sleep(10 * 1000)
        case _ =>
      }

    }
  }

  override def cancel(): Unit = ???
}
