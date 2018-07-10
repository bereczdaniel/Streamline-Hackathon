package eu.streamline.hackathon.flink.scala.job.parameter.server.IO

import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages.ParameterServerOutput
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction

class ContinuousParameterWrite(filePath: String, textOutputFormat: TextOutputFormat[ParameterServerOutput])
  extends OutputFormatSinkFunction[ParameterServerOutput](textOutputFormat) with ParameterServerOutputSink

object ContinuousParameterWrite {
  def apply(filePath: String, textOutputFormat: TextOutputFormat[ParameterServerOutput] = null): ContinuousParameterWrite = {
    val tof = new TextOutputFormat[ParameterServerOutput](new Path(filePath))
    tof.setWriteMode(FileSystem.WriteMode.OVERWRITE)
    ContinuousParameterWrite(filePath, tof)
  }
}
