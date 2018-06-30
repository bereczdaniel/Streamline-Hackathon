package eu.streamline.hackathon.flink.scala.job.logic

import eu.streamline.hackathon.flink.scala.job.utils.Types.{BasicInteraction, LightPostLoad}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}

class LightRelationMapState[T <: BasicInteraction](paramUpdate: (Double, Double) => Double) extends RichMapFunction[T, LightPostLoad]{

  @transient lazy val update: (Double, Double) => Double = paramUpdate

  lazy val state: MapState[String, Double] = getRuntimeContext
    .getMapState(new MapStateDescriptor[String, Double]("paramState", classOf[String], classOf[Double]))

  override def map(value: T): LightPostLoad = {
    if(state.contains(value.actor2))
      state.put(value.actor2, update(state.get(value.actor2), value.score))
    else
      state.put(value.actor2, value.score)

    LightPostLoad(value.actor1, value.actor2, state.get(value.actor2))
  }
}
