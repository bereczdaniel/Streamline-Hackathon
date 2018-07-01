package eu.streamline.hackathon.flink.scala.job.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.createTypeInformation

object Types {

  trait EventWithTimestamp {
    def getEventTime: Long
  }
  sealed abstract class BasicInteraction(val actor1: String, val actor2: String, val score: Double)

  case class CountryBasedInteraction(actor1CountryCode: String, actor2CountryCode: String,
                                     quadClass: Int, ts: Long, translate: Map[Int, Double], quadScore: (Int, Map[Int, Double]) => Double)
    extends BasicInteraction(actor1CountryCode, actor2CountryCode, quadScore(quadClass, translate))

  case class CountryCounter(actor1: String, actor2: String, numInteractions: Int = 1, ts: Long = 0L) extends EventWithTimestamp {
    override def getEventTime: Long = ts
  }

  sealed trait BasicPostLoad extends Product with Serializable
  case class LightPostLoad(actor1: String, actor2: String, score: Double) extends BasicPostLoad {
    override def toString: String =
      actor1 + ":" + actor2 + ":" + score

  }
  case class FullStatePostLoad(actor1: String, state: Array[(String, Double)]) extends BasicPostLoad {
  }
  case class StateRequest() extends BasicPostLoad



  case class ServerToWorker(pullId: Int, param: Double)
  case class WorkerToServer(pullId: Int)

  case class Input(actor1: Int, actor2: Int, score: Double)

  case class WorkerOut(actor1: Int, actor2: Int, param: Double)


  class LightPostLoadSchema extends DeserializationSchema[LightPostLoad] with SerializationSchema[LightPostLoad] {
    override def deserialize(message: Array[Byte]): LightPostLoad =
      deserialise(message)

    override def isEndOfStream(nextElement: LightPostLoad): Boolean =
      false

    override def serialize(element: LightPostLoad): Array[Byte] =
      serialise(element)

    override def getProducedType: TypeInformation[LightPostLoad] = {
      implicit val typeInfo: TypeInformation[LightPostLoad] = createTypeInformation[LightPostLoad]
      typeInfo
    }

  }

  def serialise(value: LightPostLoad): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(value)
    oos.close()
    stream.toByteArray
  }

  def deserialise(bytes: Array[Byte]): LightPostLoad = {
    val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val value = ois.readObject
    ois.close()
    value.asInstanceOf[LightPostLoad]
  }
}
