package eu.streamline.hackathon.flink.scala.job.parameter.server

import eu.streamline.hackathon.flink.scala.job.factors.RangedRandomFactorInitializerDescriptor
import eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic.SimpleServerLogic
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.{IDGenerator, Types}
import eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic.TrainAndEvalWorkerLogic
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object OnlineTrainAndEval {

  def main(args: Array[String]): Unit = {
    val K = 100
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)


    lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(10, -0.01, 0.01)

    val ps = new ParameterServer(
      env, "localhost:", "9093", "serverToWorkerTopic", "workerToServerTopic", "data/test_batch.csv",
      new TrainAndEvalWorkerLogic(0.01, 10, -0.01, 0.01, 50, 9),
      new SimpleServerLogic(x => factorInitDesc.open().nextFactor(x),  { (vec, deltaVec) => Types.vectorSum(vec, deltaVec)}), broadcastWorkerInput = true,
      workerInputParse =  workerInputParse, workerToServerParse =  workerToServerParse)

    val psOutput = ps.pipeline()

    psOutput.flatMap(new CoFlatMapFunction[ParameterServerOutput, ParameterServerOutput, EvaluationOutput] {
      override def flatMap1(value: ParameterServerOutput, out: Collector[EvaluationOutput]): Unit =
        value match {
          case eval: EvaluationOutput => out.collect(eval)
          case _ => throw new NotSupportedOutput
        }

      override def flatMap2(value: ParameterServerOutput, out: Collector[EvaluationOutput]): Unit = {

      }
    })
      .keyBy(_.evaluationId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(4)))
      .process(new ProcessWindowFunction[EvaluationOutput, Double, Long, TimeWindow] {
        override def process(key: Long, context: Context, elements: Iterable[EvaluationOutput], out: Collector[Double]): Unit = {
          val topK = elements.map(_.topK).fold(List())((a,b) => a ::: b).sortBy(-_._2).map(_._1).distinct.take(K)
          val targetItemId = elements.head.itemId
          out.collect(ndcg(topK, targetItemId))
        }
      })
      .writeAsText("data/output/nDCG", FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    env.execute()
  }

  def workerInputParse(line: String): WorkerInput = {
    val fields = line.split(",")
    EvaluationRequest(fields(1).toInt, fields(2).toInt, IDGenerator.next, 1.0)
  }

  def workerToServerParse(line: String): Message = {
    val fields = line.split(":")

    fields.head match {
      case "Pull" => Pull(fields(1).toInt, fields(2).toInt)
      case "Push" => Push(fields(1).toInt, fields(2).split(",").map(_.toDouble))
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
