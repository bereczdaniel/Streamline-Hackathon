package eu.streamline.hackathon.flink.scala.job.parameter.server

import java.util.Properties

import eu.streamline.hackathon.flink.scala.job.parameter.server.IO.ParameterServerOutputSink
import eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic.ServerLogic
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._
import eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic.WorkerLogic
import org.apache.flink.api.common.serialization
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector

class ParameterServer[Out <: ParameterServerOutputSink](env: StreamExecutionEnvironment,
                      host: String, port: String, serverToWorkerTopic: String, workerToServerTopic: String, path: String,
                      workerLogic: WorkerLogic, serverLogic: ServerLogic, serverOutputSink: Out = null, workerOutputSink: Out = null,
                      workerInputParse: String => WorkerInput, workerToServerParse: String => Message) {

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

  def inputStream(): DataStream[WorkerInput] = {
    env
      .readTextFile(path)
      .map(workerInputParse)
  }

  def serverToWorker(): DataStream[PullAnswer] = {
    env
      .addSource(new FlinkKafkaConsumer011[String](serverToWorkerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map(pullAnswerFromString _)
  }

  def workerToServer(): DataStream[Message] = {
    env
      .addSource(new FlinkKafkaConsumer011[String](workerToServerTopic, new serialization.SimpleStringSchema(), properties).setStartFromLatest())
      .map[Message](workerToServerParse)
      .keyBy(_.destination)
  }

  def workerInput(inputStream: DataStream[WorkerInput], serverToWorkerStream: DataStream[PullAnswer]): ConnectedStreams[WorkerInput, PullAnswer] = {
    inputStream
      .connect(serverToWorkerStream)
      .keyBy(_.id, _.workerSource)
  }

  def wl(workerInputStream: ConnectedStreams[WorkerInput, PullAnswer]): DataStream[Either[ParameterServerOutput, Message]] = {
    workerInputStream
      .flatMap(workerLogic)
  }

  def sl(serverInputStream: DataStream[Message]): DataStream[Either[ParameterServerOutput, PullAnswer]] = {
    serverInputStream
      .map(serverLogic)
  }

  def serverSink(serverLogicStream: DataStream[Either[ParameterServerOutput, PullAnswer]]): Unit = {
    serverLogicStream
      .flatMap[String]((value: Either[ParameterServerOutput, PullAnswer], out: Collector[String]) => {
      value match {
        case Right(x) =>
          val a = x.toString
          out.collect(a)
        case Left(_) =>
      }
    })
      .addSink(new FlinkKafkaProducer011[String](host + port, serverToWorkerTopic, new SimpleStringSchema()))

    serverOutputSink match {
      case null =>
      case _ =>
        serverLogicStream
          .flatMap[ParameterServerOutput]((value: Either[ParameterServerOutput, PullAnswer], out: Collector[ParameterServerOutput]) => {
          value match {
            case Left(x) => out.collect(x)
            case Right(_) =>
          }
        })
          .addSink(serverOutputSink)
    }
  }

  def workerSink(workerLogicStream: DataStream[Either[ParameterServerOutput, Message]]): Unit = {
    workerLogicStream
      .flatMap[String]((value: Either[ParameterServerOutput, Message], out: Collector[String]) => {
      value match {
        case Right(x) =>
          val a = x.toString
          out.collect(a)
        case Left(_) =>
      }
    })
      .addSink(new FlinkKafkaProducer011[String](host + port, workerToServerTopic,  new SimpleStringSchema()))

    workerOutputSink match {
      case null =>
      case _ =>
        workerLogicStream
          .flatMap[ParameterServerOutput]((value: Either[ParameterServerOutput, Message], out: Collector[ParameterServerOutput]) => {
          value match {
            case Left(x) => out.collect(x)
            case Right(_) =>
          }
        })
          .addSink(workerOutputSink)
    }
  }
}