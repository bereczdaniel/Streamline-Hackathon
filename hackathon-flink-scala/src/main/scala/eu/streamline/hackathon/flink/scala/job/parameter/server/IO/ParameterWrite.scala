package eu.streamline.hackathon.flink.scala.job.parameter.server.IO

import java.io.{File, PrintWriter}

import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages.{NotSupportedOutput, ParameterServerOutput}
import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.RecommendationSystemMessages.VectorModelOutput
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types.Parameter

import scala.collection.mutable

class ParameterWrite(file: String) extends ParameterServerOutputSink {

  val model = new mutable.HashMap[Int, Parameter]()

  override def invoke(value: ParameterServerOutput): Unit = {
    value match {
      case out: VectorModelOutput =>
        model.update(out.id, out.parameter.value)
      case _ =>
        throw new NotSupportedOutput
    }
  }

  override def close(): Unit = {
    val pw = new PrintWriter(new File(file))

    model.foreach(x =>
      pw.write(x._1 + ":" + x._2.tail.foldLeft(x._2.head.toString)((acc, c) => acc + "," + c.toString) + "\n")
    )

    pw.close()
  }

}
