package eu.streamline.hackathon.flink.scala.job.IO

import java.io.{File, PrintWriter}

import eu.streamline.hackathon.flink.scala.job.ParameterServer.{Parameter, ParameterOutput}
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

import scala.collection.mutable

class ParameterWrite[T <: ParameterOutput](file: String) extends RichSinkFunction[T]{

  val model = new mutable.HashMap[Int, Parameter]()

  override def invoke(value: T): Unit = {
    model.update(value.id, value.parameter)
  }

  override def close(): Unit = {
    val pw = new PrintWriter(new File(file))

    model.foreach(x =>
      pw.write(x._1 + ":" + x._2.tail.foldLeft(x._2.head.toString)((acc, c) => acc + "," + c.toString) + "\n")
    )

    pw.close()
  }

}
