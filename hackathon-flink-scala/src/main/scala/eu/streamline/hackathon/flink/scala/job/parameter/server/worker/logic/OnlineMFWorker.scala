package eu.streamline.hackathon.flink.scala.job.parameter.server.worker.logic

import eu.streamline.hackathon.flink.scala.job.factors.{RangedRandomFactorInitializerDescriptor, SGDUpdater}
import eu.streamline.hackathon.flink.scala.job.parameter.server.server.logic.WorkerLogic
import eu.streamline.hackathon.flink.scala.job.parameter.server.utils.Types._
import org.apache.flink.util.Collector

import scala.collection.mutable

class OnlineMFWorker(learningRate: Double, numFactors: Int, rangeMin: Double, rangeMax: Double) extends WorkerLogic{

  lazy val factorInitDesc = RangedRandomFactorInitializerDescriptor(numFactors, rangeMin, rangeMax)
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
}
