package eu.streamline.hackathon.flink.scala.job.parameter.server

import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages._
import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.RecommendationSystemMessages.{EvaluationOutput, EvaluationRequest, Pull, Push}
import eu.streamline.hackathon.flink.scala.job.parameter.server.factors.RangedRandomFactorInitializerDescriptor
import eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic.SimpleServerLogic
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types.ItemId
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.{IDGenerator, Vector}
import eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic.TrainAndEvalWorkerLogic
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object OnlineTrainAndEval {

  def main(args: Array[String]): Unit = {
    val K = 100
    val parallelism = 4
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)


    lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(10, -0.01, 0.01)

    val ps = new ParameterServer(
      env, "localhost:", "9093", "serverToWorkerTopic", "workerToServerTopic", "data/input/sliced/week_1",
      new TrainAndEvalWorkerLogic(0.4, 10, 9, -0.01, 0.01, 100, bucketSize = 10),
      new SimpleServerLogic(x => Vector(factorInitDesc.open().nextFactor(x)),  { (vec, deltaVec) => Vector.vectorSum(vec, deltaVec)}), broadcastServerToWorkers = true,
      workerInputParse =  workerInputParse, workerToServerParse =  workerToServerParse)

    val psOutput = ps.pipeline()

    val workerOut = psOutput.flatMap(new CoFlatMapFunction[ParameterServerOutput, ParameterServerOutput, EvaluationOutput] {
      override def flatMap1(value: ParameterServerOutput, out: Collector[EvaluationOutput]): Unit =
        value match {
          case eval: EvaluationOutput => out.collect(eval)
          case _ => throw new NotSupportedOutput
        }

      override def flatMap2(value: ParameterServerOutput, out: Collector[EvaluationOutput]): Unit = {

      }
    })

    workerOut.writeAsText("data/output/debugWorker", FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    val mergedTopK = workerOut
      .keyBy(_.evaluationId)
      .flatMapWithState((localTopK: EvaluationOutput, aggregatedTopKs: Option[List[EvaluationOutput]]) => {
        aggregatedTopKs match {
          case None =>
            (List.empty, Some(List(localTopK)))
          case Some(currentState) =>
            if(currentState.isEmpty)
              (List.empty, Some(List()))
            else if(currentState.length < parallelism-1) {
              (List.empty, Some(currentState.++:(List(localTopK))))
            }
            else {
              val allTopK = currentState.++(List(localTopK))
              val topK = allTopK.map(_.topK).fold(List())((a,b) => a ::: b).sortBy(-_._2).map(_._1).distinct.take(K)
              val targetItemId = allTopK.maxBy(_.itemId).itemId
              val ts = allTopK.maxBy(_.ts).ts
              (List((localTopK.evaluationId, ndcg(topK, targetItemId), ts)), Some(List()))
            }
        }
      })

    mergedTopK
      .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .process(new ProcessAllWindowFunction[(Long, Double, Long), (Int, Int, Double), TimeWindow] {
        override def process(context: Context, elements: Iterable[(Long, Double, Long)], out: Collector[(ItemId, Int, Double)]): Unit = {


          println(elements.size)
          val grouped = elements
            .groupBy(x => (x._3 / 86400).toInt)

          val output = grouped
            .map(x => (x._2.size, x._1, x._2.map(_._2).sum / x._2.size))

            output.foreach(out.collect)
        }
      })
        .print()


    env.execute()
  }

  def workerInputParse(line: String): WorkerInput = {
    val fields = line.split(",")
    EvaluationRequest(fields(1).toInt, fields(2).toInt, IDGenerator.next, 1.0, fields(0).toLong)
  }

  def workerToServerParse(line: String): Message = {
    val fields = line.split(":")

    fields.head match {
      case "Pull" => Pull(fields(1).toInt, fields(2).toInt)
      case "Push" => Push(fields(1).toInt, Vector(fields(2).split(",").map(_.toDouble)))
      case _ =>
        throw new NotSupportedMessage
        null
    }
  }

  def ndcg(topK: List[ItemId], targetItemId: Int): Double = {
    val position = topK.indexOf(targetItemId)+1
    1 / (log2(position) + 1)
  }

  def log2(x: Double): Double =
    math.log(x) / math.log(2)
}
