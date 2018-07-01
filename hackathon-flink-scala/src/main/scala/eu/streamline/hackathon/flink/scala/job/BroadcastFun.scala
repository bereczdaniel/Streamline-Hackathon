package eu.streamline.hackathon.flink.scala.job

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

object BroadcastFun {

  val data: List[(Int, Int, Double)] = scala.util.Random.shuffle((for{
    _ <- 0 until 5
    j <- 0 until 50
  } yield (j, scala.util.Random.nextInt(20), scala.util.Random.nextDouble())).toList)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val b = env.addSource(new SourceFunction[Int] {
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        val l = List(1,2,3,4)

        for(i <- l){
          ctx.collect(i)
        }
      }

      override def cancel(): Unit = ???
    }).broadcast
    env
      .fromCollection(data)
      .keyBy(_._1)
      .mapWithState((in: (Int, Int, Double), state: Option[Double]) => {
        state match {
          case None =>
            ((in._1, in._3), Some(in._3))
          case Some(i) =>
            ((in._1, in._3 + i), Some(in._3))
        }
    })
      .connect(b)
      .map(x => Left(x), y => Right(y))
      .print()

    env.execute()
  }
}
