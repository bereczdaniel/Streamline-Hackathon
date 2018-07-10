package eu.streamline.hackathon.flink.scala.job.parameter.server.communication

object BaseMessages {

  abstract class WorkerInput(val id: Int)

  trait ParameterServerOutput

  abstract class Message(val source: Int, val destination: Int)


  class NotSupportedWorkerInput extends Exception
  class NotSupportedMessage extends Exception
  class NotSupportedOutput extends Exception
}
