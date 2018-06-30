package eu.streamline.hackathon.flink.scala.job

import eu.streamline.hackathon.flink.scala.job.utils.Types.{Input, ServerToWorker, WorkerOut, WorkerToServer}
import org.apache.flink.api.common.functions.{Partitioner, RichMapFunction}
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction
import org.apache.flink.streaming.api.scala._

object IterationFun {

  val data: List[(Int, Int, Double)] = scala.util.Random.shuffle((for{
    _ <- 0 until 5
    j <- 0 until 50
  } yield (j, scala.util.Random.nextInt(20), scala.util.Random.nextDouble())).toList)


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val src = env.fromCollection(data)

    val a = src
        .map(x => Input(x._1, x._2, x._3))
      .partitionCustom(new Partitioner[Int] {
        override def partition(key: Int, numPartitions: Int): Int = key.hashCode() % numPartitions
      }, (e: Input) => e.actor1)
      .iterate((x: ConnectedStreams[Input, ServerToWorker]) => stepFunc(x), 10000L)

  }


  def stepFunc(workerIn: ConnectedStreams[Input, ServerToWorker]): (DataStream[ServerToWorker], DataStream[WorkerOut]) = {
    val worker = workerIn
      .map(new RichCoMapFunction[Input, ServerToWorker, Either[WorkerToServer, WorkerOut]] {
        override def map1(in1: Input): Either[WorkerToServer, WorkerOut] = {
          Left(WorkerToServer(in1.actor2))
        }

        override def map2(in2: ServerToWorker): Either[WorkerToServer, WorkerOut] = {
          null
        }
      })
    null
  }

}
