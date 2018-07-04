package eu.streamline.hackathon.flink.scala.job.parameter.server.IO

import java.util.Properties

import eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic.WorkerLogic
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._
import eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic.ServerLogic
import org.apache.flink.api.common.serialization
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector

class Communication(env: StreamExecutionEnvironment,
                    host: String, port: String, serverToWorkerTopic: String, workerToServerTopic: String, path: String,
                    workerLogic: WorkerLogic, serverLogic: ServerLogic,
                    serverOutFile: String, workerOutFile: String) {

  def pipeline(): Unit = {
    init()

    workerSink(
      wl(
        workerInput(
          inputStream(),
          serverToWorker()
        )
      )
    )

    serverSink(
      sl(
        workerToServer()
      )
    )

    env.execute()
  }

  lazy val properties = new Properties()

  def init(): Unit = {
    properties.setProperty("bootstrap.servers", host + port)
    properties.setProperty("group.id", "hackathon")
  }

  def inputStream(): DataStream[Rating] = {
    env
      .readTextFile(path)
      .map(line =>{
        val fields = line.split(",")
        Rating(fields(1).toInt, fields(2).toInt, 1.0)
      })
  }

  def serverToWorker(): DataStream[ServerToWorker] = {
    env
      .addSource(new FlinkKafkaConsumer011[String](serverToWorkerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map(serverToWorkerFromString _)
  }

  def workerToServer(): DataStream[WorkerToServer] = {
    env
      .addSource(new FlinkKafkaConsumer011[String](workerToServerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map[WorkerToServer]((line: String) => {
      val fields = line.split(":")

      fields.head match {
        case "Pull" => Pull(fields(1).toInt, fields(2).toInt)
        case "Push" => Push(fields(1).toInt, fields(2).split(",").map(_.toDouble))
      }

    })
      .keyBy(_.id)
  }

  def workerInput(ratingStream: DataStream[Rating], serverToWorkerStream: DataStream[ServerToWorker]): ConnectedStreams[Rating, ServerToWorker] = {
    ratingStream
      .connect(serverToWorkerStream)
      .keyBy(_.itemId, _.source)
  }

  def wl(workerInputStream: ConnectedStreams[Rating, ServerToWorker]): DataStream[Either[WorkerOut, WorkerToServer]] = {
    workerInputStream
      .flatMap(workerLogic)
  }

  def sl(serverInputStream: DataStream[WorkerToServer]): DataStream[Either[ServerOut, ServerToWorker]] = {
    serverInputStream
      .map(serverLogic)
  }

  def serverSink(serverLogicStream: DataStream[Either[ServerOut, ServerToWorker]]): Unit = {
    serverLogicStream
      .flatMap[String]((value: Either[ServerOut, ServerToWorker], out: Collector[String]) => {
      value match {
        case Right(x) =>
          val a = x.toString
          out.collect(a)
        case Left(_) =>
      }
    })
      .addSink(new FlinkKafkaProducer011[String](host + port, serverToWorkerTopic, new SimpleStringSchema()))

    serverLogicStream
      .flatMap[ServerOut]((value: Either[ServerOut, ServerToWorker], out: Collector[ServerOut]) => {
      value match {
        case Left(x) => out.collect(x)
        case Right(_) =>
      }
    })
      .writeAsText(serverOutFile, FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
  }

  def workerSink(workerLogicStream: DataStream[Either[WorkerOut, WorkerToServer]]): Unit = {
    workerLogicStream
      .flatMap[String]((value: Either[WorkerOut, WorkerToServer], out: Collector[String]) => {
      value match {
        case Right(x) =>
          val a = x.toString
          out.collect(a)
        case Left(_) =>
      }
    })
      .addSink(new FlinkKafkaProducer011[String](host + port, workerToServerTopic,  new SimpleStringSchema()))

    workerLogicStream
      .flatMap[WorkerOut]((value: Either[WorkerOut, WorkerToServer], out: Collector[WorkerOut]) => {
      value match {
        case Left(x) => out.collect(x)
        case Right(_) =>
      }
    })
      .writeAsText(workerOutFile, FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
  }
}