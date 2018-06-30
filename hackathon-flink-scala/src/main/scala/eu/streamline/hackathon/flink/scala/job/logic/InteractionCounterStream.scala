package eu.streamline.hackathon.flink.scala.job.logic

import eu.streamline.hackathon.common.data.GDELTEvent
import eu.streamline.hackathon.flink.scala.job.utils.Types.{CountryCounter, LightPostLoad}
import org.apache.flink.streaming.api.scala._

class InteractionCounterStream {

}


object InteractionCounterStream {

  def generate(src: DataStream[GDELTEvent]): DataStream[LightPostLoad] = {
    src
      .map(event => {
        val (a1,a2) = {
          if(event.actor1Code_countryCode > event.actor2Code_countryCode)
            (event.actor1Code_countryCode, event.actor2Code_countryCode)
          else
            (event.actor2Code_countryCode, event.actor1Code_countryCode)
        }
        CountryCounter(a1, a2)
      })
      .keyBy(_.actor1)
      .mapWithState((in: CountryCounter, state: Option[Int]) => {
        val cur = state.getOrElse(0) + 1

        (LightPostLoad(in.actor1, in.actor2, cur), Some(cur))
      })
  }
}