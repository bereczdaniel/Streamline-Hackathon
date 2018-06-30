package eu.streamline.hackathon.flink.scala.job.logic

import eu.streamline.hackathon.flink.scala.job.utils.Types.BasicInteraction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}

import scala.collection.mutable.ArrayBuffer

class RelationMapState[T <: BasicInteraction](paramUpdate: (Double, Double) => Double) extends RichMapFunction[T, (String, Array[(String, Double)])]{

  @transient lazy val update: (Double, Double) => Double = paramUpdate

  lazy val state: MapState[String, Double] = getRuntimeContext
    .getMapState(new MapStateDescriptor[String, Double]("paramState", classOf[String], classOf[Double]))

  override def map(in: T): (String, Array[(String, Double)]) = {
    if(state.contains(in.actor2))
      state.put(in.actor2, state.get(in.actor2) + in.score)
    else
      state.put(in.actor2, in.score)

    val res = new ArrayBuffer[(String, Double)]()

    val iter = state.iterator()

    while(iter.hasNext){
      val cur = iter.next()
      res += ((cur.getKey, cur.getValue))
    }

    (in.actor1, res.toArray)
  }


}
