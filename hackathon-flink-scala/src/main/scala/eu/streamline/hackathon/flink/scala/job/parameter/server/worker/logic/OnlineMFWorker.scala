package eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic

import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.BaseMessages._
import eu.streamline.hackathon.flink.scala.job.parameter.server.communication.RecommendationSystemMessages._
import eu.streamline.hackathon.flink.scala.job.parameter.server.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Vector
import org.apache.flink.util.Collector

import scala.collection.mutable

class OnlineMFWorker(learningRate: Double, numFactors: Int, rangeMin: Double, rangeMax: Double) extends WorkerLogic{

  lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
  lazy val SGDUpdater = new SGDUpdater(learningRate)

  val model = new mutable.HashMap[ItemId, Parameter]()
  val ratingQueue =  new mutable.HashMap[UserId, mutable.Queue[Rating]]()

  override def flatMap2(value: WorkerInput, out: Collector[Either[ParameterServerOutput, Message]]): Unit = {

    value match {
      case r: Rating =>
        ratingQueue.getOrElseUpdate(
          r.userId,
          mutable.Queue[Rating]()
        ).enqueue(r)

        out.collect(Right(Pull(r.userId, r.itemId)))

      case _ =>
        throw new NotSupportedWorkerInput
    }
  }

  override def flatMap1(value: PullAnswer, out: Collector[Either[ParameterServerOutput, Message]]): Unit = {
    val rating = ratingQueue(value.source).dequeue()
    val userVector = value.parameter
    val itemVector = model.getOrElseUpdate(rating.itemId, factorInitDesc.open().nextFactor(rating.itemId))

    val (userDelta, itemDelta) = SGDUpdater.delta(rating.rating, userVector.value, itemVector)

    model.update(rating.itemId, Vector.vectorSum(itemDelta, itemVector))

    out.collect(Right(Push(value.source, Vector(userDelta))))
    out.collect(Left(VectorModelOutput(rating.itemId, Vector(model(rating.itemId)))))
  }
}
