package eu.streamline.hackathon.flink.scala.job

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaFun {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "test")

    val stream = env
      .addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), properties))


    stream.print()

    env.execute()
  }
}
