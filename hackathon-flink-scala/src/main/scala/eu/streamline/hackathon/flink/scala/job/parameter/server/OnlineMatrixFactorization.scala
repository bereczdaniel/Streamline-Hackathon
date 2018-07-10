package eu.streamline.hackathon.flink.scala.job.parameter.server

import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages.{Message, NotSupportedMessage}
import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.RecommendationSystemMessages.{Pull, Push, Rating}
import eu.streamline.hackathon.flink.scala.job.parameter.server.factors.RangedRandomFactorInitializerDescriptor
import eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic.SimpleServerLogic
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Vector
import eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic.OnlineMFWorker
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._

object OnlineMatrixFactorization {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)


    lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(10, -0.01, 0.01)

    val ps = new ParameterServer(
      env, "localhost:", "9093", "serverToWorkerTopic", "workerToServerTopic", "data/test_batch.csv",
      new OnlineMFWorker(0.01, 10, -0.01, 0.01),
      new SimpleServerLogic(x => Vector(factorInitDesc.open().nextFactor(x)),  { (vec, deltaVec) => Vector.vectorSum(vec, deltaVec)}),
      workerInputParse =  workerInputParse, workerToServerParse =  workerToServerParse)

    val psOutput = ps.pipeline()

    psOutput.map(x => x, y => y).writeAsText("data/output/model", FileSystem.WriteMode.OVERWRITE)

    env.execute()
  }

  def workerInputParse(line: String): Rating = {
    val fields = line.split(",")
    Rating(fields(1).toInt, fields(2).toInt, 1.0)
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
}
