package eu.streamline.hackathon.flink.scala.job.IO

import java.util.Date

import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.operations.GDELTInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._

class GDELTSource {

}

object GDELTSource {

  implicit val typeInfo: TypeInformation[GDELTEvent] = createTypeInformation[GDELTEvent]
  implicit val dateInfo: TypeInformation[Date] = createTypeInformation[Date]

  def read(env: StreamExecutionEnvironment, path: String): DataStream[GDELTEvent] =
    env.readFile[GDELTEvent](new GDELTInputFormat(new Path(path)), path)

}
