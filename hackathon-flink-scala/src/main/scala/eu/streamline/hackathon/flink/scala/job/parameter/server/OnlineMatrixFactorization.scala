package eu.streamline.hackathon.flink.scala.job.parameter.server

import eu.streamline.hackathon.flink.scala.job.factors.RangedRandomFactorInitializerDescriptor
import eu.streamline.hackathon.flink.scala.job.parameter.server.IO.Communication
import eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic.SimpleServerLogic
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types
import eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic.OnlineMFWorker
import org.apache.flink.streaming.api.scala._

object OnlineMatrixFactorization {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(10, -0.01, 0.01)

    val ps = new Communication(
      env, "localhost:", "9093", "serverToWorkerTopic", "workerToServerTopic", "data/test_batch.csv",
      new OnlineMFWorker(0.01, 10, -0.01, 0.01),
      new SimpleServerLogic(x => factorInitDesc.open().nextFactor(x),  { (vec, deltaVec) => Types.vectorSum(vec, deltaVec)}),
      "data/output/server", "data/output/worker")

    ps.pipeline()
  }
}
