package eu.streamline.hackathon.flink.scala.job.logic.state

import java.util
import java.util.function.Consumer

import eu.streamline.hackathon.flink.scala.job.utils.Types.{BasicInteraction, FullStatePostLoad}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}

import scala.collection.mutable.ArrayBuffer

class RelationMapState[T <: BasicInteraction](paramUpdate: (Double, Double) => Double) extends RichMapFunction[T, FullStatePostLoad]{

  @transient lazy val update: (Double, Double) => Double = paramUpdate

  lazy val state: MapState[String, Double] = getRuntimeContext
    .getMapState(new MapStateDescriptor[String, Double]("paramState", classOf[String], classOf[Double]))

  override def map(in: T): FullStatePostLoad = {
    if(state.contains(in.actor2))
      state.put(in.actor2, update(state.get(in.actor2), in.score))
    else
      state.put(in.actor2, update(in.score, 0.0))

    val res = new ArrayBuffer[(String, Double)]()

    state
      .entries()
      .forEach(new Consumer[util.Map.Entry[String, Double]] {
        override def accept(t: util.Map.Entry[String, Double]): Unit =
          res += ((t.getKey, t.getValue))
      })

    FullStatePostLoad(in.actor1, res.toArray)
  }
}
