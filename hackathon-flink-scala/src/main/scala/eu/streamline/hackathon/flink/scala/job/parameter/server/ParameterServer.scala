package eu.streamline.hackathon.flink.scala.job.parameter.server

import java.util.Properties

import eu.streamline.hackathon.flink.scala.job.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._
import org.apache.flink.api.common.serialization
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector

import scala.collection.mutable

object ParameterServer {

  def main(args: Array[String]): Unit = {
    onlineMFPipeline(
      "data/test_batch.csv", "data/output/worker", "data/output/server",
      "workerToServerTopic", "serverToWorkerTopic", "localhost", ":9093",
      0.1, 10, -0.01, 0.01)
  }

  def onlineMFPipeline(dataPath: String, workerOutFile: String, serverOutFile: String,
                       workerToServerTopic: String, serverToWorkerTopic: String, host: String, port: String,
                       learningRate: Double, numFactors: Int, rangeMin: Double, rangeMax: Double): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    //val workerEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //val serverEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream =
      env
        .readTextFile(dataPath)
        .map(line =>{
          val fields = line.split(",")
          Rating(fields(1).toInt, fields(2).toInt, 1.0)
        })

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", host + port)
    properties.setProperty("group.id", "hackathon")


    val serverToWorker: DataStream[ServerToWorker] = env
      .addSource(new FlinkKafkaConsumer011[String](serverToWorkerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map(serverToWorkerFromString _)



    val workerToServer = env
      .addSource(new FlinkKafkaConsumer011[String](workerToServerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map[WorkerToServer]((line: String) => {
        val fields = line.split(":")

        fields.head match {
          case "Pull" => Pull(fields(1).toInt, fields(2).toInt)
          case "Push" => Push(fields(1).toInt, fields(2).split(",").map(_.toDouble))
        }

      })
      .keyBy(_.id)



    val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)



    //WorkerLogic

    val workerLogic = dataStream
      .connect(serverToWorker)
      .keyBy(_.itemId, _.source)
      .flatMap(new RichCoFlatMapFunction[Rating, ServerToWorker, Either[WorkerOut, WorkerToServer]] {

        lazy val SGDUpdater = new SGDUpdater(learningRate)

        val model = new mutable.HashMap[ItemId, Parameter]()
        val ratingQueue =  new mutable.HashMap[UserId, mutable.Queue[Rating]]()

        override def flatMap1(value: Rating, out: Collector[Either[WorkerOut, WorkerToServer]]): Unit = {
          ratingQueue.getOrElseUpdate(
            value.userId,
            mutable.Queue[Rating]()
            ).enqueue(value)

          out.collect(Right(Pull(value.userId, value.itemId)))
        }

        override def flatMap2(value: ServerToWorker, out: Collector[Either[WorkerOut, WorkerToServer]]): Unit = {
          val rating = ratingQueue(value.id).dequeue()
          val userVector = value.parameter
          val itemVector = model.getOrElseUpdate(rating.itemId, factorInitDesc.open().nextFactor(rating.itemId))

          val (userDelta, itemDelta) = SGDUpdater.delta(rating.rating, userVector, itemVector)

          model.update(rating.itemId, vectorSum(itemDelta, itemVector))

          out.collect(Right(Push(value.id, userDelta)))
          out.collect(Left(WorkerOut(rating.itemId, model(rating.itemId))))
        }
      })

    workerLogic
      .flatMap[String]((value: Either[WorkerOut, WorkerToServer], out: Collector[String]) => {
        value match {
          case Right(x) =>
            val a = x.toString
            out.collect(a)
          case Left(_) =>
        }
      })
      .addSink(new FlinkKafkaProducer011[String](host + port, workerToServerTopic,  new SimpleStringSchema()))

    workerLogic
      .flatMap[WorkerOut]((value: Either[WorkerOut, WorkerToServer], out: Collector[WorkerOut]) => {
        value match {
          case Left(x) => out.collect(x)
          case Right(_) =>
        }
      })
      .writeAsText(workerOutFile, FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)


    // ServerLogic

    val serverLogic = workerToServer
      .mapWithState((msg: WorkerToServer, state: Option[Parameter]) => {
        msg match {
          case Push(id, deltaParam) =>
            val newParam = vectorSum(state.get, deltaParam)
            (Left(ServerOut(id, newParam)), Some(newParam))

          case Pull(id, source) =>
            val param = state.getOrElse(factorInitDesc.open().nextFactor(id))
            (Right(ServerToWorker(id, source, param)), Some(param))
        }
      })

    serverLogic
      .flatMap[String]((value: Either[ServerOut, ServerToWorker], out: Collector[String]) => {
        value match {
          case Right(x) =>
            val a = x.toString
            out.collect(a)
          case Left(_) =>
        }
      })
      .addSink(new FlinkKafkaProducer011[String](host + port, serverToWorkerTopic, new SimpleStringSchema()))

    serverLogic
      .flatMap[ServerOut]((value: Either[ServerOut, ServerToWorker], out: Collector[ServerOut]) => {
      value match {
        case Left(x) => out.collect(x)
        case Right(_) =>
      }
    })
        .writeAsText(serverOutFile, FileSystem.WriteMode.OVERWRITE)
        .setParallelism(1)

    env.execute()
  }
}
